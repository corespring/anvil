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

    he = (h,cb) =>
      @test_hash_exists(h,cb)

    hashes = @hashes()

    console.log "hashes length: #{hashes.length}"
    async.map @hashes(), he, (err, results) ->
      missing = []

      console.log "received results : #{results}"
      for r in results
        missing.push(r.hash) unless r.exists
      cb missing


  manifest_url: ->
    "#{process.env.ANVIL_HOST}/manifest/#{@id}.json"

module.exports.init = (manifest) ->
  console.log "create new manifest..."
  new Manifest(manifest)
