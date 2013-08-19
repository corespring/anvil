async    = require("async")
fs       = require("fs")
http     = require("http")
https    = require("https")
mkdirp   = require("mkdirp")
os       = require("os")
path     = require("path")
program  = require("commander")
url      = require("url")
util     = require("util")

http.globalAgent.maxSockets = 50

program
  .version(require("#{__dirname}/../package.json").version)
  .usage('[options] <manifest> <target>')

datastore_hash_fetchers = (manifest, dir) ->
  fetchers = {}
  for name, file_manifest of manifest when file_manifest.hash
    do (name, file_manifest) =>
      fetchers[file_manifest.hash] = (async_cb) =>
        console.log "hash fetcher for : #{name}"
        filename = "#{dir}/#{name}"
        mkdirp path.dirname(filename), =>
          fetch_url "#{process.env.ANVIL_HOST}/file/#{file_manifest["hash"]}", filename, (err) ->
            console.log "#{filename} file fetched - chmod"
            async_cb(err) if err?
            fs.chmod filename, file_manifest.mode, (err) ->
              async_cb(err) if err?
              console.log "#{filename} file fetched - utimes"
              fs.utimes filename, file_manifest.mtime, file_manifest.mtime, (err) ->
                console.log "#{filename} utimes done"
                async_cb err, true

datastore_link_fetchers = (manifest, dir) ->
  fetchers = {}
  for name, file_manifest of manifest when file_manifest.link
    do (name, file_manifest) =>
      fetchers[file_manifest.link] = (async_cb) =>

        "link fetcher for: #{name}"

        console.log "linking", name, file_manifest
        filename = "#{dir}/#{name}"
        mkdirp path.dirname(filename), =>
          console.log "link", filename, file_manifest.link
          fs.symlink "#{dir}/#{file_manifest.link}", filename, ->

            console.log "#{name} sym link created"
            fs.chmod filename, file_manifest.mode, (err) ->
              console.log "#{name} chmod completed"
              async_cb null, true

fetch_url = (url, filename, cb) ->
  console.log "#{filename} -> create write stream"

  createStream =  ->
    try
      fs.createWriteStream filename
    catch e
      console.log e
    finally
      cb "Error creating writesStream #{filename} #{e}"

  file    = createStream filename
  options = require("url").parse(url)
  client  = if options.protocol is "https:" then https else http
  get     = client.request options, (res) ->
    res.on "data",  (chunk) ->
      console.log "#{filename} [fetch_url] data received - write"
      file.write chunk
    res.on "end", ->
      file.end()
      cb null
  get.on "error", (err) ->
    console.log "error fetching #{url}: #{err}, retrying"
    file.end()
    fetch_url url, filename, cb
  get.end()

module.exports.execute = (args) ->
  program.parse(args)

  console.log "[download manifest] execute"

  console.log "[download manifest] args: #{args}"
  file = program.args[0]
  console.log "[download manifest] read file: #{file}"

  fs.readFile program.args[0], (err, data) ->
    manifest = JSON.parse(data)
    mkdirp program.args[1]

    async.parallel datastore_hash_fetchers(manifest, program.args[1]), (err, results) ->
      console.log "----------- run hash fetchers"
      if err?
        console.log(err)
      else
        console.log "_________ run link fetchers"
        async.parallel datastore_link_fetchers(manifest, program.args[1]), (err, results) ->

          if err? then console.log err else console.log "complete"
