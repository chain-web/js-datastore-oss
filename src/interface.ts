import type OSS from "ali-oss";

export interface OssDatastoreOptionsI {
  createIfMissing: boolean; // auto create bucket when there is no bucket
  oss?: OSS;
  ossOption?: OSS.Options
}
