import IPFS from "ipfs-core";
import toBuffer from "it-to-buffer";
import { createOssRepo, OssLock } from "datastore-oss";
import OSS from "ali-oss";
import { randomBytes } from "crypto";


async function main() {
  const oss = new OSS({
    region: 'region',
    accessKeyId: 'AccessKey',
    accessKeySecret: 'AccessKeySecret',
    bucket: 'bucket-name'
  });

  // Prevents concurrent access to the repo, you can also use the memory or fs locks
  // bundled with ipfs-repo though they will not prevent processes running on two
  // machines accessing the same repo in parallel
  const repoLock = new OssLock(oss);

  // Create the repo
  const ossRepo = createOssRepo("/", oss, repoLock);

  // Create a new IPFS node with our oss backed Repo
  console.log("Start ipfs");
  const node = await IPFS.create({
    repo: ossRepo,
  });

  // Test out the repo by sending and fetching some data
  console.log("IPFS is ready");

  try {
    const version = await node.version();
    console.log("Version:", version.version);

    // Once we have the version, let's add a file to IPFS
    const { path, cid } = await node.add({
      path: "data.txt",
      content: Buffer.from(randomBytes(1024 * 25)),
    });

    console.log("\nAdded file:", path, cid);

    // Log out the added files metadata and cat the file from IPFS
    const data = await toBuffer(node.cat(cid));

    // Print out the files contents to console
    console.log(`\nFetched file content containing ${data.byteLength} bytes`);
  } catch (err) {
    // Log out the error
    console.log("File Processing Error:", err);
  }
  // After everything is done, shut the node down
  // We don't need to worry about catching errors here
  console.log("\n\nStopping the node");
  await node.stop();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
