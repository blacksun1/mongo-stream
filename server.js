"use strict";

const MongoClient = require("mongodb").MongoClient;
const assert = require("assert");
const uuid = require("uuid");
const _ = require("highland");

// Connection URL
const url = "mongodb://localhost:27017/streams";

// Use connect method to connect to the Server
MongoClient.connect(url, (err, db) => {
  assert.equal(null, err);
  console.log("Connected correctly to server");

  const col = db.collection("streams");

  // // Insert a single document
  // col.insert([
  //     {"_id": uuid.v4(), "a": 1},
  //     {"_id": uuid.v4(), "a": 2},
  //     {"_id": uuid.v4(), "a": 3},
  //     {"_id": uuid.v4(), "a": 4},
  //     {"_id": uuid.v4(), "a": 5},
  //     {"_id": uuid.v4(), "a": 6},
  //     {"_id": uuid.v4(), "a": 7},
  //     {"_id": uuid.v4(), "a": 8},
  //     {"_id": uuid.v4(), "a": 9},
  //     {"_id": uuid.v4(), "a": 10}
  //   ], function(err, r) {
  //     assert.equal(null, err);
  //     assert.equal(10, r.result.n);

      const fs = require("fs");
      const writable = fs.createWriteStream("file.txt");
      const mongoReadStream = _(col.find({}).stream());

      // Setup the the stream.
      // This does not do any work - just sets it up.
      const mappedStream = mongoReadStream
        // .map(JSON.stringify)
        .map(x => {
          x.mapped = true;
          return x;
        })
        // .map(x => JSON.stringify(x, null, 2) + "\n")

      // Then this actually consumes the stream.
      // jsonStream.fork()
      //   .map(console.log);

      // As we are just observing, these will not start
      // the stream - just observe the results as stuff
      // happens.
      mappedStream.observe().each(_.log)

      mappedStream.observe().done(() => {
        db.close(() => console.log("DB closed"));
      });

      // And then this is the main stream - it will start
      // and consume the stream and is responsible for
      // handling back-pressure.
      mappedStream
        // .map(x => JSON.parse(x))
        .map(x => {
          x.cake = "Yum";
          return JSON.stringify(x);
        })
        // .each(_.log);
        // .pipe(process.stdout);
        .pipe(writable);
  // });
});
