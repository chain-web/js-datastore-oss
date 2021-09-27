"use strict";

import PATH from "path";
import { fromString as uint8ArrayFromString } from "uint8arrays";
import type { LockCloser, RepoLock } from "ipfs-repo";
import type OSS from "ali-oss";
/**
 * Uses an object in an oss bucket as a lock to signal that an IPFS repo is in use.
 * When the object exists, the repo is in use.
 * You would normally use this to make sure multiple IPFS nodes don’t use the same oss bucket as a datastore at the same time.
 */

export class OssLock implements RepoLock {
  constructor(ossClient: OSS) {
    this.ossClient = ossClient;
  }
  ossClient: OSS;

  /**
   * Returns the location of the lock file given the path it should be located at
   */
  getLockfilePath(dir: string) {
    return PATH.join(dir, "repo.lock");
  }

  /**
   * Creates the lock. This can be overridden to customize where the lock should be created
   */
  async lock(dir: string): Promise<LockCloser> {
    const lockPath = this.getLockfilePath(dir);

    let alreadyLocked, err;
    try {
      alreadyLocked = await this.locked(dir);
    } catch (e) {
      err = e;
    }
    if (err || alreadyLocked) {
      throw new Error("The repo is already locked");
    }

    // There's no lock yet, create one
    await this.ossClient.put(lockPath, Buffer.from(uint8ArrayFromString("")));

    return this.getCloser(lockPath);
  }

  /**
   * Returns a LockCloser, which has a `close` method for removing the lock located at `lockPath`
   */
  getCloser(lockPath: string): LockCloser {
    const closer = {
      /**
       * Removes the lock file. This can be overridden to customize how the lock is removed. This
       * is important for removing any created locks.
       */
      close: async () => {
        try {
          await this.ossClient.delete(lockPath);
        } catch (err: any) {
          // TODO 处理无文件
          if (err.statusCode !== 404) {
            throw err;
          }
        }
      },
    };
    let cleaning = false;
    const cleanup = async (err: Error) => {
      if (cleaning) {
        return;
      }
      if (err instanceof Error) {
        console.log("\nAn Uncaught Exception Occurred:\n", err);
      } else if (err) {
        console.log("\nReceived a shutdown signal:", err);
      }

      console.log("\nAttempting to cleanup gracefully...");

      try {
        cleaning = true;
        await closer.close();
      } catch (e: any) {
        console.log("Caught error cleaning up: %s", e.message);
      }
      cleaning = false;
      console.log("Cleanup complete, exiting.");
      process.exit();
    };

    // listen for graceful termination
    if (process && process.on) {
      // in NodeJs
      console.log("datastore-oss-lock listening for exit gc");
      process.on("exit", cleanup);
      process.on("SIGTERM", cleanup);
      process.on("SIGINT", cleanup);
      process.on("SIGHUP", cleanup);
      process.on("uncaughtException", cleanup);
    } else {
      // in Browser
    }

    return closer;
  }

  /**
   * Calls back on whether or not a lock exists.
   */
  async locked(dir: string): Promise<boolean> {
    try {
      await this.ossClient.get(this.getLockfilePath(dir));
    } catch (err: any) {
      if (err.code === "NoSuchKey") {
        return false;
      }
      throw err;
    }

    return true;
  }
}
