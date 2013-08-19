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


prepare_file = (name, file_manifest, dir) ->

  acync_fn = (async_cb) =>
    filename = "#{dir}/#{name}"
    mkdirp path.dirname(filename), =>
      fetch_url "#{process.env.ANVIL_HOST}/file/#{file_manifest["hash"]}", filename, (err) ->
        fs.chmod filename, file_manifest.mode, (err) ->
            fs.utimes filename, file_manifest.mtime, file_manifest.mtime, (err) ->
              async_cb err, true
  acync_fn


datastore_hash_fetchers = (manifest, dir) ->
  fetchers = {}
  for name, file_manifest of manifest when file_manifest.hash
    do (name, file_manifest) =>
      fetchers[file_manifest.hash] = (async_cb) =>
        filename = "#{dir}/#{name}"
        mkdirp path.dirname(filename), =>
          fetch_url "#{process.env.ANVIL_HOST}/file/#{file_manifest["hash"]}", filename, (err) ->
            async_cb(err) if err?
            fs.chmod filename, file_manifest.mode, (err) ->
              async_cb(err) if err?
              fs.utimes filename, file_manifest.mtime, file_manifest.mtime, (err) ->
                async_cb err, true

datastore_link_fetchers = (manifest, dir) ->
  fetchers = {}
  for name, file_manifest of manifest when file_manifest.link
    do (name, file_manifest) =>
      fetchers[file_manifest.link] = (async_cb) =>
        filename = "#{dir}/#{name}"
        mkdirp path.dirname(filename), =>
          fs.symlink "#{dir}/#{file_manifest.link}", filename, ->
            fs.chmod filename, file_manifest.mode, (err) ->
              console.log "#{name} chmod completed"
              async_cb err, true

fetch_url = (url, filename, cb) ->

  createStream =  ->
    try
      fs.createWriteStream filename
    catch e
      console.log e
      console.trace
    finally
      cb "Error creating writesStream #{filename}"

  writeChunk = (f, c) ->
    try
      f.write c
    catch e
      console.log e
      console.trace
    finally
      cb "Error writing chunk #{filename}"

  fileEnd = (f) ->
    try
      f.end()
    catch e
      console.log e
      console.trace e
    finally
      cb "Error calling end() #{filename}"

  file    = createStream filename
  options = require("url").parse(url)
  client  = if options.protocol is "https:" then https else http
  get     = client.request options, (res) ->
    res.on "data",  (chunk) -> writeChunk file, chunk
    res.on "end", ->
      fileEnd(file)
      cb null
  get.on "error", (err) ->
    console.log "error fetching #{url}: #{err}, retrying"
    file.end()
    fetch_url url, filename, cb
  get.end()

module.exports.execute = (args) ->

  try
    program.parse(args)

    console.log "[download manifest] args: #{args}"

    file = program.args[0]

    console.log "[download manifest] read file: #{file}"

    fs.readFile program.args[0], (err, data) ->
      manifest = JSON.parse(data)
      base_dir = program.args[1]
      mkdirp base_dir


      prep_functions = []
      for name, file_manifest of manifest
        prep_functions.push (prepare_file(name, file_manifest, base_dir))

      console.log "prep_functions #{prep_functions.length}"

      run_functions = prep_functions[1..10]

      async.parallelLimit run_functions, 50, (err, results) ->
        console.log err
        console.log results
      ###
      async.parallelLimit datastore_hash_fetchers(manifest, program.args[1]), 10, (err, results) ->
        if err?
          console.log(err)
        else
          async.parallelLimit datastore_link_fetchers(manifest, program.args[1]), 10, (err, results) ->
            if err? then console.log(err) else console.log "complete"
      ###

  catch e
    console.log e
    console.trace
    throw "Error running execute #{e}"
