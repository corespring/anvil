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


prepare_file = (name, file_manifest, dir, task_cb) ->
  filename = "#{dir}/#{name}"
  mkdirp path.dirname(filename), =>
    fetch_url "#{process.env.ANVIL_HOST}/file/#{file_manifest["hash"]}", filename, (err) ->
      fs.chmod filename, file_manifest.mode, (err) ->
        fs.utimes filename, file_manifest.mtime, file_manifest.mtime, (err) ->
          task_cb err, true

link_file = (name, file_manifest, dir, task_cb) ->
  filename = "#{dir}/#{name}"
  mkdirp path.dirname(filename), =>
    fs.symlink "#{dir}/#{file_manifest.link}", filename, ->
      fs.chmod filename, file_manifest.mode, (err) ->
        task_cb err, true



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
      cb "Error creating writesStream #{filename}"

  writeChunk = (f, c) ->
    try
      f.write c
    catch e
      console.log e
      console.trace
      cb "Error writing chunk #{filename}"

  fileEnd = (f) ->
    try
      f.end()
    catch e
      console.log e
      console.trace e
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


      tasks = []
      for name, file_manifest of manifest
        tasks.push ( { name: name, manifest: file_manifest, base_dir: base_dir } )

      console.log "prep_functions #{tasks.length}"

      task_subset = tasks #[1..500]

      q = async.queue( (task, cb) ->
          console.log ">> dl + link: #{task.name}"
          onFilePrepped = (err, isSuccess) -> link_file( task.name, task.manifest, task.base_dir, cb)
          prepare_file( task.name, task.manifest, task.base_dir, onFilePrepped)
      , 50)

      q.push task_subset

      q.drain = ->
        console.log('all items have been processed')

      #async.parallelLimit task_subset, 10, (err, results) ->
      #  if err? then console.log err
      #  console.log results
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
