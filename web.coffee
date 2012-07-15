coffee   = require("coffee-script")
crypto   = require("crypto")
express  = require("express")
fs       = require("fs")
manifest = require("manifest")
storage  = require("storage").init()
util     = require("util")

app = express.createServer(
  express.logger(),
  express.cookieParser(),
  express.bodyParser())

app.get "/cache/:id.tgz", (req, res) ->
  storage.get "/cache/#{req.params.id}.tgz", (err, get) ->
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.put "/cache/:id.tgz", (req, res) ->
  storage.create_stream "/cache/#{req.params.id}.tgz", fs.createReadStream(req.files.data.path), (err) ->
    res.send("ok")

app.get "/file/:hash", (req, res) ->
  storage.get "/hash/#{req.params.hash}", (err, get) ->
    res.writeHead get.statusCode,
      "Content-Length": get.headers["content-length"]
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.post "/file/:hash", (req, res) ->
  sha  = crypto.createHash("sha256")
  file = fs.createReadStream(req.files.data.path)
  file.on "data", (data) ->
    sha.update data
  file.on "end", ->
    if req.params.hash == sha.digest("hex")
      storage.create_stream "/hash/#{req.params.hash}", fs.createReadStream(req.files.data.path), (err) ->
        res.send "ok"
    else
      res.send "file does not match hash", 403

app.post "/manifest", (req, res) ->
  manifest.init(JSON.parse(req.body.manifest)).save (err, id) ->
    res.contentType "application/json"
    res.send JSON.stringify({ id:id })

app.post "/build", (req, res) ->
  options =
    buildpack: req.body.buildpack
    cache:     req.body.cache
    env:       req.body.env
  require("builder").init().build req.body.source, options, (build, builder) ->
    res.writeHead 200
      "Content-Type":      "text/plain"
      "Transfer-Encoding": "chunked"
      "X-Cache-Url":       builder.cache_url
      "X-Slug-Url":        builder.slug_url()
    build.on "data", (data)   -> res.write(data)
    build.on "end", (success) -> res.end()

app.post "/manifest/build", (req, res) ->
  options =
    buildpack: req.body.buildpack
    cache:     req.body.cache
    env:       req.body.env
  manifest.init(JSON.parse(req.body.manifest)).build options, (build, builder) ->
    res.writeHead 200
      "Content-Type":      "text/plain"
      "Transfer-Encoding": "chunked"
      "X-Cache-Url":       builder.cache_url
      "X-Slug-Url":        builder.slug_url()
    build.on "data", (data)   -> res.write(data)
    build.on "end", (success) -> res.end()

app.post "/manifest/diff", (req, res) ->
  manifest.init(JSON.parse(req.body.manifest)).missing_hashes (hashes) ->
    res.contentType "application/json"
    res.send JSON.stringify(hashes)

app.get "/manifest/:id.json", (req, res) ->
  storage.get "/manifest/#{req.params.id}.json", (err, get) =>
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

app.get "/slugs/:id.img", (req, res) ->
  storage.get "/slug/#{req.params.id}.img", (err, get) ->
    get.on "data", (chunk) -> res.write chunk
    get.on "end",          -> res.end()

port = process.env.PORT || 5000

app.listen port, ->
  console.log "listening on port #{port}"
