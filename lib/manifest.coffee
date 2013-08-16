async   = require("async")
uuid    = require("node-uuid")

class Manifest

  constructor: (@manifest) ->
    @builder = require("./builder").init()
    @storage = require("./storage").init()
    @id      = uuid.v4()

  hashes: ->
    object.hash for name, object of @manifest when object.hash

  save: (cb) ->

    console.log "Manifest.save..."
    manifest = new Buffer(JSON.stringify(@manifest), "binary")
    options  =
      "Content-Length": manifest.length,
      "Content-Type":  "application/json"

    console.log "@storage save: ${@id}"
    @storage.create "/manifest/#{@id}.json", manifest, options, (err) =>
      cb err, @manifest_url()

  test_hash_exists: (h, cb) ->
    @storage.exists "/hash/#{h}", (err, exists) ->
      cb(null,{exists: exists, hash: h})
      null
    null

  missing_hashes: (cb) ->

    console.log "Manifest - missing_hashes..."

    he = (h,cb) =>
      @test_hash_exists(h,cb)

    hashes = @hashes()

    console.log "hashes length: #{hashes.length}"
    async.map @hashes(), he, (err, results) ->
      missing = []

      console.log "received results : #{results}"
      for r in results
        console.log "-----------"
        console.log r
        console.log "-----------"
        missing.push(r.hash) unless r.exists
      cb missing

  test_datastore_presence: (hash, cb) ->
    @storage.exists "/hash/#{hash}", (err, exists) ->
      @cb_count = @cb_count + 1
      console.log @cb_count
      cb(exists)

  datastore_testers: ->
    h = @hashes()

    console.log "hashes: ", h
    console.log "length: #{h.length}"
    h.reduce (ax, hash) =>
      ax[hash] = (async_cb) =>
        @test_datastore_presence hash, (exists) ->
          console.log "exists? : #{exists}"
          async_cb(null, exists)
      ax
    ,{}

  manifest_url: ->
    "#{process.env.ANVIL_HOST}/manifest/#{@id}.json"

module.exports.init = (manifest) ->
  console.log "create new manifest..."
  new Manifest(manifest)
