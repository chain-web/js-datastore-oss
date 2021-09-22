# Full OSS Repo

This example leverages the code from https://github.com/ipfs/js-ipfs/tree/master/examples/ipfs-101,
but uses an instantiated aliyun OSS instance to serve as the entire backend for ipfs.

## Running

The OSS parameters must be updated with an existing Bucket and credentials with access to it:

```js
const oss = new OSS({
  region: "region",
  accessKeyId: "myaccesskey",
  accessKeySecret: "mysecretkey",
  bucket: "bucket-name",
});
```

Once the OSS instance has its needed data, you can run the example:

```
npm install
node index.js
```
