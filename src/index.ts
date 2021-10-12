import { Buffer } from "buffer";
import filter from "it-filter";
import { Key } from "interface-datastore";
import {
  BaseDatastore,
  Batch,
  KeyQuery,
  Options,
  Query,
} from "datastore-core/base";
import * as Errors from "datastore-core/errors";
import { fromString as unint8arrayFromString } from "uint8arrays";
import toBuffer from "it-to-buffer";
import type { OssDatastoreOptionsI } from "./interface";
import OSS from "ali-oss";
export { OssLock } from "./lib/ossLock";
export { createOssRepo } from "./lib/create-oss-repo";

/**
 * A datastore backed by the file system, save to aliyun oss.
 *
 * Keys need to be sanitized before use, as they are written
 * to the file system as is.
 */
export class OssDatastore extends BaseDatastore {
  constructor(path: string, opts: OssDatastoreOptionsI) {
    super();

    this.path = path
      // aliyun not suport /path/, use path/
      .replace(/^\//, "");
    this.opts = opts;
    const { createIfMissing = false, oss, ossOption } = opts;
    if (oss) {
      this.ossClient = oss;
    } else {
      if (!ossOption) {
        throw new Error(
          "oss and ossOptions must have one. See the README for examples."
        );
      }
      if (typeof ossOption?.bucket !== "string") {
        throw new Error(
          "ossOption.bucket must be supplied. See the README for examples."
        );
      }
      this.ossClient = new OSS({
        ...ossOption,
      });
    }

    if (typeof createIfMissing !== "boolean") {
      throw new Error(
        `createIfMissing must be a boolean but was (${typeof createIfMissing}) ${createIfMissing}`
      );
    }
    this.createIfMissing = createIfMissing;
  }

  path: string;
  opts: OssDatastoreOptionsI;
  createIfMissing: boolean;
  ossClient: OSS;

  /**
   * Returns the full key which includes the path to the ipfs store
   */
  _getFullKey(key: Key) {
    // Avoid absolute paths with oss
    this.opts.hasLog && console.log(key.toString());
    return [this.path, key.toString()].join("/").replace(/\/\/+/g, "/");
  }

  /**
   * Store the given value under the key.
   */
  async put(key: Key, val: Uint8Array): Promise<void> {
    try {
      await this.ossClient.put(
        this._getFullKey(key),
        Buffer.from(val, val.byteOffset, val.byteLength)
      );
    } catch (err: any) {
      // auto create bucket
      if (err.status === 404 && this.createIfMissing) {
        const bucket = (this.ossClient as any).options.bucket;
        await this.ossClient.putBucket(bucket, {
          // https://help.aliyun.com/document_detail/32071.html
          storageClass: "Standard",
          acl: "public-read",
          dataRedundancyType: "LRS",
          timeout: 60000,
        });
        this.ossClient.useBucket(bucket);
      }
      throw Errors.dbWriteFailedError(err);
    }
  }

  /**
   * Read from oss.
   */
  async get(key: Key): Promise<Uint8Array> {
    try {
      const data = await this.ossClient.get(this._getFullKey(key));

      if (!data.content) {
        throw new Error("Response had no content");
      }

      // If a body was returned, ensure it's a Uint8Array
      if (data.content instanceof Uint8Array) {
        return data.content;
      }

      if (typeof data.content === "string") {
        return unint8arrayFromString(data.content);
      }

      if (data.content instanceof Blob) {
        const buf = await data.content.arrayBuffer();

        return new Uint8Array(buf, 0, buf.byteLength);
      }

      return await toBuffer(data.content);
    } catch (err: any) {
      if (err.code === "NoSuchKey" || err.code === "NoSuchKeyError") {
        throw Errors.notFoundError(err);
      }
      throw err;
    }
  }

  /**
   * Check for the existence of the given key.
   */
  async has(key: Key) {
    try {
      await this.ossClient.head(this._getFullKey(key));

      return true;
    } catch (err: any) {
      if (err.code === "NoSuchKey") {
        return false;
      }
      throw err;
    }
  }

  /**
   * Delete the record under the given key.
   */
  async delete(key: Key) {
    try {
      await this.ossClient.delete(this._getFullKey(key));
    } catch (err: any) {
      throw Errors.dbDeleteFailedError(err);
    }
  }

  /**
   * Create a new batch object.
   */
  batch(): Batch {
    const puts: { key: Key; value: Uint8Array }[] = [];
    const deletes: Key[] = [];
    return {
      put(key: Key, value: Uint8Array) {
        puts.push({ key: key, value: value });
      },
      delete(key: Key) {
        deletes.push(key);
      },
      commit: async () => {
        const putOps = puts.map((p) => this.put(p.key, p.value));
        const delOps = deletes.map((key) => this.delete(key));
        await Promise.all(putOps.concat(delOps));
      },
    };
  }

  /**
   * Recursively fetches all keys from oss
   */
  async *_listKeys(
    params: { prefix?: string; StartAfter?: string },
    options?: Options
  ): AsyncGenerator<Key, void, undefined> {
    try {
      const data = await this.ossClient.list(
        {
          prefix: params.prefix,
          "max-keys": 100,
          marker: params.StartAfter,
        },
        {}
      );
      if (options && options.signal && options.signal.aborted) {
        return;
      }

      if (!data || !data.objects?.length) {
        throw new Error(`Not found: ${params.prefix}`);
      }

      for (const d of data.objects) {
        if (!d.name) {
          throw new Error(`Not found: ${params.prefix} name`);
        }
        // Remove the path from the key
        yield new Key(d.name.slice(this.path.length), false);
      }

      // If we didn't get all records, recursively query
      if (data.isTruncated) {
        // If NextMarker is absent, use the key from the last result
        params.StartAfter = data.nextMarker;

        // recursively fetch keys
        yield* this._listKeys(params);
      }
    } catch (err: any) {
      console.log("err------.code");
      console.log(err.code);
      console.log(err);
      throw new Error(err.code);
    }
  }

  /**
   *
   */
  async *_all(q: Query, options: Options) {
    for await (const key of this._allKeys({ prefix: q.prefix }, options)) {
      try {
        const res = {
          key,
          value: await this.get(key),
        };

        yield res;
      } catch (err: any) {
        // key was deleted while we are iterating over the results
        if (err.statusCode !== 404) {
          throw err;
        }
      }
    }
  }

  /**
   *
   */
  async *_allKeys(
    q: KeyQuery,
    options: Options
  ): AsyncGenerator<Key, void, undefined> {
    const prefix = [this.path, q.prefix || ""].join("/").replace(/\/\/+/g, "/");
    // Get all the keys via list object, recursively as needed
    let it = this._listKeys(
      {
        prefix: prefix,
      },
      options
    );

    if (q.prefix != null) {
      it = filter(it, (k) => k.toString().startsWith(`${q.prefix || ""}`));
    }

    yield* it;
  }

  /**
   * This will check the oss bucket to ensure access and existence
   */
  async open() {
    try {
      await this.ossClient.head(this.path);
    } catch (err: any) {
      if (err.code !== "NoSuchKey") {
        throw Errors.dbOpenFailedError(err);
      }
    }
  }

  async close() {}
}
