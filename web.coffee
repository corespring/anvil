builder  = require("./lib/builder")
coffee   = require("coffee-script")
crypto   = require("crypto")
express  = require("express")
fs       = require("fs")
log      = require("./lib/logger")
manifest = require("./lib/manifest")
storage  = require("./lib/storage").init()
util     = require("util")

require("http").globalAgent.maxSockets = 50

express.logger.format "method", (req, res) ->
  req.method.toLowerCase()

express.logger.format "url", (req, res) ->
  req.url.replace('"', '&quot')

express.logger.format "user-agent", (req, res) ->
  (req.headers["user-agent"] || "").replace('"', '')

app = express.createServer(
  express.logger
    buffer: false
    format: "subject=\"http\" method=\":method\" url=\":url\" status=\":status\" elapsed=\":response-time\" from=\":remote-addr\" agent=\":user-agent\""
  express.bodyParser())

app.get "/", (req, res) ->
  res.send "tweaks branch - ok"

app.get "/heartbeat", (req, res) ->
  res.send "ok"

app.post "/build", (req, res) ->
  log "api.build"
    type:      "url"
    source:    req.body.source
    buildpack: req.body.buildpack
    app:       req.headers["x-heroku-app"]
    user:      req.headers["x-heroku-user"]
    (logger) ->
      builder.init().build_request req, res, logger

app.get "/cache/:id.tgz", (req, res) ->
  storage.get "/cache/#{req.params.id}.tgz", (err, get) ->
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.put "/cache/:id.tgz", (req, res) ->
  console.log "put /cache/:id.tgz..."
  console.log req.files

  file_type = req.files.data["type"]
  filesize = req.files.data.size
  path = req.files.data.path
  url = "/cache/#{req.params.id}.tgz"

  storage.create_stream url, filesize, file_type, fs.createReadStream(path), (err) ->
    if err? then res.status(400).send(err) else res.send "ok"

app.get "/exit/:id", (req, res) ->
  storage.get "/exit/#{req.params.id}", (err, get) ->
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.get "/file/:hash", (req, res) ->
  log "api.file.get", hash:req.params.hash, (logger) ->
    storage.get "/hash/#{req.params.hash}", (err, get) ->
      res.writeHead get.statusCode,
        "Content-Length": get.headers["content-length"]
      get.on "data", (chunk) -> res.write chunk
      get.on "end",          -> logger.finish(); res.end()

app.post "/file/:hash", (req, res) ->
  log "api.file.post", hash:req.params.hash, (logger) ->
    storage.verify_hash req.files.data.path, req.params.hash, (err) ->
      return res.send(err, 403) if err

      file_type = req.files.data["type"]
      filesize = req.files.data.size
      path = req.files.data.path
      url = "/hash/#{req.params.hash}"

      storage.create_stream url, filesize, file_type, fs.createReadStream(path), (err) ->
        if err? then res.status(400).send(err) else res.send "ok"

app.post "/manifest", (req, res) ->
  manifest.init(JSON.parse(req.body.manifest)).save (err, manifest_url) ->
    res.header "Location", manifest_url
    res.send "ok"

app.post "/manifest/build", (req, res) ->
  log "api.build"
    type:      "manifest"
    buildpack: req.body.buildpack
    app:       req.headers["x-heroku-app"]
    user:      req.headers["x-heroku-user"]
    (logger) ->
      manifest.init(JSON.parse(req.body.manifest)).save (err, manifest_url) ->
        delete req.body.manifest
        req.body.source = manifest_url
        builder.init().build_request req, res, logger

app.post "/manifest/diff", (req, res) ->
  console.log "received request for diffs..."
  bodyManifest = JSON.parse(req.body.manifest)
  console.info manifest
  manifest.init(bodyManifest).missing_hashes (hashes) ->
    missing = JSON.stringify(hashes)
    console.log "[web.coffee] /manifest/diff - hashes: #{missing}"
    res.contentType "application/json"
    res.send missing

app.get "/manifest/:id.json", (req, res) ->
  storage.get "/manifest/#{req.params.id}.json", (err, get) =>
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.get "/slugs/:id.deb", (req, res) ->
  storage.get "/slug/#{req.params.id}.deb", (err, get) ->
    res.writeHead get.statusCode,
      "Content-Length": get.headers["content-length"]
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.get "/slugs/:id.img", (req, res) ->
  storage.get "/slug/#{req.params.id}.img", (err, get) ->
    res.writeHead get.statusCode,
      "Content-Length": get.headers["content-length"]
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.get "/slugs/:id.tgz", (req, res) ->
  storage.get "/slug/#{req.params.id}.tgz", (err, get) ->
    res.writeHead get.statusCode,
      "Content-Length": get.headers["content-length"]
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

port = process.env.PORT || 5000

app.listen port, ->
  console.log "listening on port #{port}"
