/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.executor

import java.net.URL

import io.snappydata.cluster.ExecutorInitiator

import org.apache.spark.util.{SparkExitCode, ShutdownHookManager}
import org.apache.spark.{Logging, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.sql.SnappyContext

class SnappyCoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostPort: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv)
    extends CoarseGrainedExecutorBackend(rpcEnv, driverUrl,
      executorId, hostPort, cores, userClassPath, env) {

  override def onStop() {
    SnappyContext.clearStaticArtifacts()
    exitWithoutRestart()
  }

  override def onStart(): Unit = {
    super.onStart()
    val ueh = new SnappyUncaughtExceptionHandler(this)
    Thread.setDefaultUncaughtExceptionHandler(ueh)
  }

  /**
   * Snappy addition (Replace System.exit with exitExecutor). We could have
   * added functions calling System.exit to SnappyCoarseGrainedExecutorBackend
   * but those functions will have to be brought in sync with CoarseGrainedExecutorBackend
   * after every merge.
   */
  override def exitExecutor(): Unit = {
    exitWithoutRestart()
    // Executor may fail to connect to the driver because of
    // https://issues.apache.org/jira/browse/SPARK-9820 and
    // https://issues.apache.org/jira/browse/SPARK-8592. To overcome such
    // issues, try restarting the executor
    logWarning("Executor has failed to start: Restarting.")
    ExecutorInitiator.restartExecutor()
  }

  def exitWithoutRestart(): Unit = {
    if (executor != null) {
      // kill all the running tasks
      // InterruptThread is set as true.
      executor.killAllTasks(true)
      executor.stop()
    }
    // stop the actor system
    stop()
    if (rpcEnv != null) {
      rpcEnv.shutdown()
    }

    SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
  }
}

class SnappyUncaughtExceptionHandler(
    val eb: SnappyCoarseGrainedExecutorBackend) extends Thread.UncaughtExceptionHandler with Logging {
  override def uncaughtException(thread: Thread, exception: Throwable) {
    try {
      // Make it explicit that uncaught exceptions are thrown when container is shutting down.
      // It will help users when they analyze the executor logs
      val inShutdownMsg = if (ShutdownHookManager.inShutdown()) "[Container in shutdown] " else ""
      val errMsg = "Uncaught exception in thread "
      logError(inShutdownMsg + errMsg + thread, exception)

      // We may have been called from a shutdown hook. If so, we must not call System.exit().
      // (If we do, we will deadlock.)
      if (!ShutdownHookManager.inShutdown()) {
        if (exception.isInstanceOf[OutOfMemoryError]) {
          System.exit(SparkExitCode.OOM)
        } else {
          eb.exitExecutor()
        }
      }
    } catch {
      case oom: OutOfMemoryError => Runtime.getRuntime.halt(SparkExitCode.OOM)
      case t: Throwable => Runtime.getRuntime.halt(SparkExitCode.UNCAUGHT_EXCEPTION_TWICE)
    }
  }

  def uncaughtException(exception: Throwable) {
    uncaughtException(Thread.currentThread(), exception)
  }
}
