import { OssDatastore } from "../index";
import { createRepo, RepoLock } from "ipfs-repo";
import { BlockstoreDatastoreAdapter } from "blockstore-datastore-adapter";
import { ShardingDatastore } from "datastore-core/sharding";
import { NextToLast } from "datastore-core/shard";
import * as raw from "multiformats/codecs/raw";
import * as json from "multiformats/codecs/json";
import * as dagPb from "@ipld/dag-pb";
import * as dagCbor from "@ipld/dag-cbor";
import type OSS from "ali-oss";
import type { BlockCodec } from "ipfs-core-utils/src/multicodecs";
import type { loadCodec } from "ipfs-repo/src/types";

/**
 * A convenience method for creating an oss backed IPFS repo
 */
export const createOssRepo = (path: string, oss: OSS, repoLock: RepoLock) => {
  const storeConfig = {
    oss,
    createIfMissing: true,
  };

  /**
   * These are the codecs we want to support, you may wish to add others
   */
  const codecs: Record<string | number, BlockCodec> = {
    [raw.code]: raw,
    [raw.name]: raw,
    [json.code]: json,
    [json.name]: json,
    [dagPb.code]: dagPb,
    [dagPb.name]: dagPb,
    [dagCbor.code]: dagCbor,
    [dagCbor.name]: dagCbor,
  };

  const loadCodecFunc: loadCodec = async (codeOrName) => {
    if (codecs[codeOrName]) {
      return codecs[codeOrName];
    }

    throw new Error(`Unsupported codec ${codeOrName}`);
  };

  return createRepo(
    path,
    loadCodecFunc,
    {
      root: new ShardingDatastore(
        new OssDatastore(path, storeConfig),
        new NextToLast(2)
      ),
      blocks: new BlockstoreDatastoreAdapter(
        new ShardingDatastore(
          new OssDatastore(`${path}blocks`, storeConfig),
          new NextToLast(2)
        )
      ),
      datastore: new ShardingDatastore(
        new OssDatastore(`${path}datastore`, storeConfig),
        new NextToLast(2)
      ),
      keys: new ShardingDatastore(
        new OssDatastore(`${path}keys`, storeConfig),
        new NextToLast(2)
      ),
      pins: new ShardingDatastore(
        new OssDatastore(`${path}pins`, storeConfig),
        new NextToLast(2)
      ),
    },
    {
      repoLock,
    }
  );
};
