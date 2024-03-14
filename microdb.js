import { promises as fs, createReadStream, createWriteStream } from 'fs'
import crypto from 'crypto'
import readline from 'readline'
import path from 'path'

let globalObjectId = crypto.randomBytes(8)
function newObjectId() {
  for (let i = 7; i >= 0; i--)
    if (++globalObjectId[i] < 256)
      break
  return (new Date().getTime() / 1000 | 0).toString(16) + globalObjectId.toString('hex')
}

export function clone (obj, fields) {
  function copyValue (v, fields) {
    if (typeof v === 'object') {
      if (Array.isArray(v))
        return v.map(item => copyValue(item, fields))
      if (!Object.getPrototypeOf(v) || Object.getPrototypeOf(v) === Object.prototype)
        return copy(v, {}, fields)
      if (v.constructor.name === 'ObjectId')
        return v.valueOf()
      if (v instanceof Date)
        return new Date(v)
      console.warn('Unknonw ducument property type: ' + v.constructor.name)
    }
    return v
  }
  function copy (obj, res, fields) {
    for (const n in obj) {
      if (!fields || fields[n])
        res[n] = copyValue(obj[n], fields && fields[n])
    }
    return res
  }
  return copyValue(obj, fields)
}

export function serialize (doc) {
  function copy (doc, cdoc) {
    for (const n in doc) {
      const v = doc[n]
      if (typeof v === 'object') {
        if (Array.isArray(v)) {
          copy(v, cdoc[n] = [])
        } else if (!Object.getPrototypeOf(v) || Object.getPrototypeOf(v) === Object.prototype) {
          copy(v, cdoc[n] = {})
        } else if (v.constructor.name === 'ObjectId') {
          cdoc[n] = v.valueOf()
        } else if (v instanceof Data) {
          cdoc[n] = { '@type': 'date', v: v.toJSON() }
        } else {
          console.warn('Unknonw ducument property type: ' + v.constructor.name)
        }
      } else
        cdoc[n] = doc[n]
    }
    return cdoc
  }
  return JSON.stringify(copy(doc, {}))
}

export function deserialize (doc) {
  function fix (doc) {
    for (const n in doc) {
      const v = doc[n]
      if (typeof v === 'object') {
        if (Array.isArray(v)) {
          fix(v)
        } else {
          const t = v['@type']
          if (t) {
            switch (t) {
              case 'date': doc[n] = new Date(v.v); break
              default:
                throw new Error('Unknown object type')
            }
          } else
            fix(v)
        }
      }
    }
    return doc
  }
  return fix(JSON.parse(doc))
}

export class Queue {
  constructor () {
    this._head = null
    this._tail = null
    this.length = 0
  }

  push (o) {
    this.length++
    if (!this._tail) {
      this._head = this._tail = { o: o }
    } else {
      const node = { o: o, p: this._tail }
      this._tail.n = node
      this._tail = node
    }
    return o
  }

  pop () {
    if (this._tail) {
      const node = this._tail
      this._tail = this._tail.p
      delete node.p
      if (this._tail)
        delete this._tail.n
      else
        this._head = this._tail
      this.length--
      return node.o
    }
  }

  add (o) {
    return this.push(o)
  }

  shift () {
    if (this._head) {
      const node = this._head
      this._head = this._head.n
      delete node.n
      if (this._head)
        delete this._head.p
      else
        this._tail = this._head
      this.length--
      return node.o
    }
  }

  unshift (o) {
    this.length++
    if (!this._head) {
      this._head = this._tail = { o: o }
    } else {
      const node = { o: o, n: this._head }
      this._head.p = node
      this._head = node
    }
    return o
  }

  clear () {
    this.node = null
    this.length = 0
  }

  first () {
    return this._head && this._head.o
  }

  first () {
    return this._tail && this._tail.o
  }

  isEmpty () {
    return !this._head
  }

  *[Symbol.iterator] () {
    let item = this._head
    while (item) {
      const o = item.o
      item = item.n
      yield o
    }
  }

  toArray () {
    const arr = []
    for (let item = this._head; item; item = item.n)
      arr.push(item.o)
    return arr
  }

  forEach (cb, self) {
    for (let item = this._head, i = 0; item; i++) {
      const o = item.o
      item = item.n
      if (cb.call(self, item.o, i, this) === false)
        break
    }
  }
}

export class Cursor {
  /**
   * 
   * @param {Collection} col 
   * @param {object} query 
   * @param {object} options 
   */
  constructor (col, query, options) {
    this.collection = col
    this.options = options || {}
    this.query = query
  }

  limit (n) {
    this.options.limit = n
  }

  sort (obj) {
    this.options.sort = obj
  }

  skip (n) {
    this.options.skip = n
  }

  async toArray () {
    if (!this.collection._isReady)
      await this.collection._ready()

    const res = []
    for (const doc of this.collection._query(this.query, this.options))
      res.push(clone(doc, this.options.fields))
    return res
  }

  async next () {
    if (!this.collection._isReady)
      await this.collection._ready()

    // not secure over simultaneous inserts/deletes
    if (!this._query)
      this._query = this.collection._query(this.query, this.options)
    return clone(this._query.next().value, this.options.fields)
  }

  async forEach (cb) {
    if (!this.collection._isReady)
      await this.collection._ready()

    return new Promise((resolve, reject) => {
      process.nextTick(() => {
        let count = 0
        for (const doc of this.collection._query(this.query, this.options)) {
          count++
          try {
            if (cb(clone(doc, this.options.fields)) === false)
              break
          } catch (e) {
            reject(e)
          }
        }
        resolve(count)
      })
    })
  }

  close () {
    delete this._query
    delete this.collection
  }
}

export class Collection {
  /**
   * @param {object} options
   * @param {MicroDB} options.db
   * @param {string} options.name
   */
  constructor (options) {
    this.db = options.db
    this.name = options.name
    this._isReady = !this.db
    this._items = new Map()
    this._queryCache = {}
    this._nextId = 1
  }

  _ready (cb) {
    if (this._isReady) {
      if (cb) return cb()
      return Promise.resolve()
    }
    if (!this._openev) {
      this._openev = []
      return this.db._action(this, 'open')
        .then(() => {
          this._isReady = true
          const arr = this._openev
          delete this._openev
          arr.forEach(cb => cb())
        })
    }
    if (!cb)
      return new Promise(resolve => this._openev.push(resolve))
    this._openev.push(cb)
  }

  _query (query, options) {
    function generateFunction (query) {
      const args = {}, argsList = []
      let argId = 0
      function getVar (prefix, name, ext) {
        name = (prefix ? prefix + '.' : '') + name + (ext ? '.' + ext : '')
        let n = args[name]
        if (!n) {
          n = 'arg' + (++argId)
          args[name] = n
          argsList.push(n + '=obj.' + name)
        }
        return n
      }
      function _filter (prefix, query, oper) {
        const list = []
        if (Array.isArray(query)) {
          query.forEach((v, i) => {
            list.push(_filter(prefix + '[' + i + ']', v, ' && '))
          })
          return list.length ? '(' + list.join(oper) + ')' : 'true'
        }
        for (const name in query) {
          const v = query[name]
          switch (name) {
            case '$not':
              list.push('!(' + _filter((prefix ? prefix + '.' : '') + name, v, ' && ') + ')')
              break
            case '$and':
              list.push('(' + _filter((prefix ? prefix + '.' : '') + name, v, ' && ') + ')')
              break
            case '$or':
              list.push('(' + _filter((prefix ? prefix + '.' : '') + name, v, ' || ') + ')')
              break
            default:
              if (name[0] === '$')
                console.warn('operator ' + n + ' not supported')
              else {
                if (typeof (v) === 'object') {
                  const sublist = []
                  for (const q in v) {
                    switch (q) {
                      case '$eq':
                        sublist.push('doc.' + name + '===' + getVar(prefix, name, q))
                        break
                      case '$ne':
                        sublist.push('doc.' + name + '!==' + getVar(prefix, name, q))
                        break
                      case '$in':
                        if (!Array.isArray(v[q]))
                          throw new Error('invalid operator ' + q + ' usage')
                        sublist.push(getVar(prefix, name, q) + '.includes(doc.' + name + ')')
                        break
                      case '$nin':
                        if (!Array.isArray(v[q]))
                          throw new Error('invalid operator ' + q + ' usage')
                        sublist.push(getVar(prefix, name, q) + '.includes(doc.' + name + ')')
                        break
                      case '$gt':
                        sublist.push('doc.' + name + '>' + getVar(prefix, name, q))
                        break
                      case '$gte':
                        sublist.push('doc.' + name + '>=' + getVar(preifx, name, q))
                        break
                      case '$lt':
                        sublist.push('doc.' + name + '<' + getVar(prefix, name, q))
                        break
                      case '$lte':
                        sublist.push('doc.' + name + '<=' + getVar(prefix, name, q))
                        break
                      case '$regex':
                        sublist.push('doc.' + name + '.match(' + getVar(preifx, name, q) + ')')
                        break
                      case '$like':
                        sublist.push('doc.' + name + '.includes(' + getVar(preifx, name, q) + ')')
                        break
                      case '$nlike':
                        sublist.push('!doc.' + name + '.includes(' + getVar(prefix, name, q) + ')')
                        break
                      default:
                        console.warn('operator ' + q + ' not supported')
                    }
                  }
                  list.push(sublist.sort().join(' && '))
                } else {
                  if (name !== '_id')
                    list.push('doc.' + name + '===' + getVar(prefix, name))
                }
              }
          }
        }
        return list.sort().join(oper || ' && ')
      }
      const filter = _filter('', query), vars = argsList.length ? 'const ' + argsList.join(',') + ';' : ''
      return vars + 'return function*query(iter){let item,doc;while(item=iter.next().value){doc=item[1];' + (filter ? 'if(' + filter + ')' : '') + '{if(skip)skip--;else{yield doc;if(--limit<=0)return}}}}'
    }

    const skip = Math.max(options.skip || 0, 0),
      limit = Math.max(options.limit || 99999999, 1) + skip
    let filter, items

    if (query) {
      const queryFunc = generateFunction(query)
      if (query._id) {
        const doc = this._items.get(query._id.valueOf())
        if (!queryFunc.startsWith('const'))
          return (function* (doc) { if (doc) yield doc })(doc)
        items = (function* (v) { yield v })(doc && [doc._id.valueOf(), doc])
      }
      if (!filter) {
        if (!(filter = this._queryCache[queryFunc])) {
          filter = this._queryCache[queryFunc] = new Function('obj', 'skip', 'limit', 'iter', queryFunc)
        }
      }
    } else {
      if (!(filter = this._queryCache['*']))
        filter = this._queryCache['*'] = new Function('obj', 'skip', 'limit', 'iter', generateFunction({}))
    }
    return filter(query, skip, limit)(items || this._items.entries())
  }

  compactCollection () {
    if (!this.db) return Promise.resolve()
    return this.db._action(this, 'compact')
  }

  drop () {
    this._items.clear()
    if (!this.db) return Promise.resolve()
    return this.db._action(this, 'drop')
  }

  _queryObject(query, data) {
    for (const item in query) {
      const v = query[item]
      if (typeof v === 'object') {
        for (const oper in v) {
          switch (oper) {
            case '$eq':
              if (data[item] !== v[oper])
                return false
              break
            case '$ne':
              if (data[item] === v[oper])
                return false
              break
           case '$in':
              if (!Array.isArray(v[oper]))
                throw new Error('invalid operator ' + oper + ' usage')
              if (!v[oper].includes(data[item]))
                return false
              break
            case '$nin':
              if (!Array.isArray(v[oper]))
                throw new Error('invalid operator ' + oper + ' usage')
              if (v[oper].includes(data[item]))
                return false
              break
            case '$gt':
              if (data[item] <= v[oper])
                return false
              break
            case '$gte':
              if (data[item] < v[oper])
                return false
              break
            case '$lt':
              if (data[item] >= v[oper])
                return false
              break
            case '$lte':
              if (data[item] > v[oper])
                return false
              break
            case '$like':
              if (!data[item].includes(v[oper]))
                return false
              break
            case '$nlike':
              if (data[item].includes(v[oper]))
                return false
              break
            case '$regex':
              if (!data[item].match(v[oper]))
                return false
              break
            default:
              if (typeof data[item] !== 'object' || data[item][oper] !== v[oper])
                return false
              break
          }
        }
      } else
        if (v !== data[item])
          return false
    }
    return true
  }

  _equal (doc, upd) {
    let count = 0
    for (const n in upd) {
      count++
      if (doc[n] !== upd[n]) {
        if (typeof upd[n] === 'object' && typeof doc[n] === 'object' && this._equal(doc[n], upd[n]))
          continue
        return false
      }
    }
    return count === Object.keys(doc).length
  }

  _update (doc, upd, insert) {
    let res = false
    if (!doc || !upd)
      return res
    for (const n in upd) {
      const v = upd[n]
      if (n[0] === '$') {
        switch (n) {
          case '$set':
            for (const nn in v) {
              if (doc[nn] !== v[nn]) {
                doc[nn] = v[nn]
                res = true
              }
            }
            break
          case '$setOnInsert':
            if (insert)
              for (const nn in v) {
              if (doc[nn] !== v[nn]) {
                  doc[nn] = v[nn]
                  res = true
                }
              }
            break
          case '$unset':
            for (const nn in v) {
              if (Object.hasOwnProperty(doc, nn)) {
                delete doc[nn]
                res = true
              }
            }
            break
          case '$inc':
            for (const nn in v) {
              doc[nn] = (doc[nn] || 0) + 1
              res = true
            }
            break;
          case '$push':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              const vv = Array.isArray(v[nn]) ? v[nn] : [v[nn]]
              arr.push.apply(arr, vv)
              res = true
            }
            break
          case '$addToSet':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              const vv = Array.isArray(v[nn]) ? v[nn] : [v[nn]]
              for (const subitem of vv) {
                if (!arr.includes(subitem)) {
                  arr.push(subitem)
                  res = true
                }
              }
            }
            break
          case '$pop':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              if (arr.length !== 0) {
                const vv = v[nn]
                if (vv === -1)
                  arr.shift()
                else
                  arr.pop()
                res = true
              }
            }
            break
          case '$pullAll':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              if (arr.length !== 0) {
                doc[nn] = []
                res = true
              }
            }
            break
          case '$pull':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              // filter items
              const filter = v[nn]
              const arrOut = arr.filter(item => this._queryObject(filter, item))
              if (arrOut.length !== arr.length) {
                doc[nn] = arrOut
                res = true
              }
            }
            break;
          default:
            throw new Error('operator ' + n + ' not supported')
        }
      } else {
        if (doc[n] !== upd[n]) {
          doc[n] = upd[n]
          res = true
        }
      }
    }
    return res
  }

  async countDocuments (query, options = {}) {
    if (!this._isReady)
      await this._ready()

    let count = this._items.size
    if (query && Object.keys.length) {
      count = 0
      for (const iter = this._query(query, options); iter.next().value; count++) { }
    }
    return count
  }

  find (query, fields, options) {
    return new Cursor(this, query, { fields, ...options })
  }

  async findOne (query, fields, options = {}) {
    if (!this._isReady)
      await this._ready()
    return clone(this._query(query, options).next().value, fields)
  }

  async _modify (query, update, options, cb) {
    if (!this._isReady)
      await this._ready()

    if (!update && !options.remove && !query)
      throw new TypeError()
    if (!query && update._id)
      query = { _id: update._id }

    let count = 0, doc //, resdoc
    for (const iter = this._query(query, options); doc = iter.next().value;) {
      count++
      cb(count, doc, false)
      if (options.remove) {
        this._items.delete(doc._id)
        if (this.db)
          this.db._action(this, 'remove', doc._id)
        doc = null
      } else {
        if (options.replace) {
          if (!update._id)
            update._id = doc._id
          if (doc._id.valueOf() !== update._id.valueOf())
            throw new Error('replace requires _id to be the same')
          if (!this._equal(doc, update)) {
            doc = update
            this._items.set(doc._id.valueOf(), doc)
            if (this.db)
              this.db._action(this, 'update', doc)
          }
        } else if (this._update(doc, update)) {
          this._items.set(doc._id.valueOf(), doc)
          if (this.db)
            this.db._action(this, 'update', doc)
        }
      }
      cb(count, doc, true)
    }
    if (!count && options.upsert) {
      doc = { _id: newObjectId() }
      if (query)
        for (const n in query)
          if (n[0] !== '$')
            doc[n] = query[n]
      this._update(doc, update, true)
      this._items.set(doc._id.valueOf(), doc)
      if (this.db)
        this.db._action(this, 'insert', doc)
      cb(0, doc, true)
    }
  }

  async findAndModify (options = {}) {
    options.limit = 1
    let resdoc
    await this._modify(options.query, options.update, options, (pos, doc, after) => {
      if (!pos || (options.new && after) || (!options.new && !after))
        resdoc = clone(doc, options.fields)
    })
    return resdoc
  }

  async insertOne (doc) {
    return (await this.insertMany([doc])).insertedIds[0]
  }

  async insertMany (docs) {
    if (!this._isReady)
      await this._ready()

    const insertedIds = []

    docs.forEach(doc => {
      if (doc._id && this._items.get(doc._id.valueOf()))
        throw new Error('Document dupplicate')
      if (!doc._id)
        doc._id = newObjectId()
      this._items.set(doc._id.valueOf(), doc)
      if (this.db)
        this.db._action(this, 'insert', doc)
      insertedIds.push(doc._id)
    })

    return { insertedIds: insertedIds }
  }

  updateOne (query, doc, options = {}) {
    return this.updateMany(query, doc, { ...options, limit: 1 })
  }

  async updateMany (query, update, options = {}) {
    let upsertedId, modifiedCount = 0
    await this._modify(query, update, options, (pos, doc, after) => {
      if (!pos)
        upsertedId = doc._id
      else
        modifiedCount = pos
    })
    return { upsertedId, modifiedCount }
  }

  replaceOne (query, doc, options) {
    return this.updateMany(query, doc, { ...options, replace: true, limit: 1 })
  }

  deleteOne (query, options) {
    return this.deleteMany(query, { ...options, limit: 1 })
  }

  async deleteMany (query, options = {}) {
    if (!this._isReady)
      await this._ready()

    let count = 0, doc
    for (const iter = this._query(query, options); doc = iter.next().value; count++) {
      this._items.delete(doc._id)
      if (this.db)
        this.db._action(this, 'remove', doc._id)
    }
    return { deletedCount: count }
  }
}

export class StorageAdapter {
  constructor (db, options) {
  }

  openDb () {
    return Promise.resolve()
  }

  closeDb () {
    return Promise.resolve()
  }

  dropDb () {
    return Promise.resolve()
  }

  action (name, col, data) {
    return Promise.resolve()
  }
}

export class FileAdapter extends StorageAdapter {
  constructor (db, options) {
    super(db, options)
    this._db = db
    this.dir = path.resolve(options.path || 'db')
    this._execqueue = new Queue()
  }

  async openDb () {
    try {
      await fs.mkdir(this.dir, { recursive: true })
    } catch (e) {
      console.error('Create directory error: ' + this.dir, e)
    }
    this.isConnected = true
  }

  closeDb () {
    return Promise.resolve()
  }

  dropDb () {
    return fs.rm(this.dir, { recursive: true, force: true })
  }

  action (name, col, data) {
    return new Promise((resolve, reject) => {
      this._execqueue.push({ collection: col, action: name, data: clone(data), resolve, reject })
      this._next()
    })
  }

  async drop (col) {
    this.close(col.name)
    try {
      await fs.unlink(path.join(this.dir, col.name + '.db'))
    } catch { }
  }

  async open (col) {
    if (col._isReady)
      return

    const fileName = path.resolve(path.join(this.dir, col.name + '.db'))

    try {
      await fs.stat(fileName)
      await new Promise(resolve => {
        const frs = createReadStream(fileName), reader = readline.createInterface(frs), items = new Map()
        let fileLines = 0, maxId = 0

        const readLine = line_1 => {
          try {
            fileLines++
            if (line_1.startsWith('{"$$')) {
              const obj = JSON.parse(line_1)
              if (obj.$$id)
                maxId = parseInt(obj.$$id) || maxId
              if (obj.$$delete) {
                if (typeof obj.$$delete === 'number' && obj.$$delete > maxId)
                  maxId = obj.$$delete
                items.delete(obj.$$delete)
              }
            } else
            if (line_1.startsWith('{')) {
              const obj = deserialize(line_1)
              if (obj._id) { // document must contain _id
                items.set(obj._id, obj)
                if (typeof obj._id === 'number' && obj._id > maxId)
                  maxId = obj._id
              }
            }
          } catch {
          }
        }

        const readDone = (e_1) => {
          col._nextId = maxId + 1
          col._fileLines = fileLines
          col._items = items
          frs.close()
          resolve()
        }

        reader.on('line', readLine)
        reader.on('close', readDone)
        frs.on('error', readDone)
      })
    } catch { }
    col._file = createWriteStream(fileName, { flags: 'a' })
    this._execqueue.processing = false
    this._next()
  }

  close (col) {
    return new Promise(resolve => {
      const f = col._file
      if (f) {
        this._isReady = false
        col._file = undefined
        col._fileLines = 0
        f.close(resolve)
      } else
        resolve()
    })
  }

  compact (col, options) {
    const size = col._items.size
    if (!col._file || (options === 'auto' && col._fileLines <= size * 4) ||
      col._fileLines === size) {
      return Promise.resolve()
    }
    return new Promise(resolve => {
      const fileName = path.resolve(path.join(this.dir, col.name + '.db'))
      if (col._file)
        col._file.close()
      col._file = createWriteStream(fileName + '.tmp', { flags: 'w' })
      let lines = 0
      const keys = []
      for (const key of col._items.keys())
        keys.push(key)
      const iter = (function* (arr) { yield* arr })(keys),
        nextWrite = () => {
          while (true) {
            const id = iter.next().value
            if (!id) {
              col._file.close(() => {
                col._file = undefined
                fs.rename(fileName + '.tmp', fileName).then(() => {
                  col._file = createWriteStream(fileName, { flags: 'a' })
                  col._fileLines = lines
                  resolve()
                })
              })
              return
            }
            const doc = col._items.get(id)
            if (doc) {
              lines++
              col._file.write(serialize(doc) + '\n', nextWrite)
              return true
            }
          }
        }
      nextWrite()
      col._file.once("error", () => {
        // file write failed
        col._file = null
        resolve()
      })
    })
  }

  _write (col, doc) {
    if (!col._file)
      return Promise.resolve()

    return new Promise(resolve => {
      col._fileLines++
      col._file.write((typeof (doc) === 'string' ? doc : serialize(doc)) + '\n', () => {
        resolve()
      })
      if (col._fileLines > col._items.size * 4)
        this._execqueue.unshift({
          collection: col,
          action: 'compact',
          data: 'auto',
          resolve: () => { },
          reject: () => { }
        })
    })
  }

  insert (col, data) {
    return this._write(col, data)
  }

  update (col, data) {
    return this._write(col, data)
  }

  remove (col, data) {
    return this._write(col, JSON.stringify({$$delete: data.valueOf()}))
  }

  _next () {
    if (!this._execqueue.processing && this._execqueue.length)
      process.nextTick(this._process.bind(this))
  }

  _process () {
    if (this._execqueue.processing || !this._execqueue.length) return

    this._execqueue.processing = true

    const item = this._execqueue.shift()
    if (!item.action) {
      item.resolve()
      this._execqueue.processing = false
      return this._process()
    }
    this[item.action](item.collection, item.data).then(res => {
      item.resolve(res)
    }).catch(err => {
      item.reject(err)
    }).then(() => {
      this._execqueue.processing = false
      this._next()
    })
  }
}

export class MicroDB {
  constructor (url, options = {}) {
    url = typeof url === 'string' ? new URL(url) : url
    this._collections = {}
    const adapter = options.adapter || (url.host && FileAdapter) || StorageAdapter
    options.path = options.path || decodeURI(url.host + url.pathname)
    /** @type {StorageAdapter} */
    this._adapter = new adapter(this, options)
  }

  _action (col, action, data) {
    return this._adapter.action(action, col, data)
  }

  open () {
    return this._adapter.openDb().then(() => this)
  }

  async close () {
    // close all collections
    const promises = []
    for (const n in this._collections)
      promises.push(this._action(this._collections[n], 'close'))
    this._collections = {}
    await Promise.all(promises)
    return this._adapter.closeDb()
  }

  collection (name, options) {
    return Promise.resolve(this._collections[name] = new Collection(Object.assign({ db: this, name }, options || {})))
  }

  collections () {
    const res = []
    for (const col in this._collections)
      res.push(this._collections[col])
    return Promise.resolve(res)
  }

  async collectionNames () {
    const names = []
    try {
      const dirs = await fs.readdir(this._dir)
      dirs.forEach(name => {
        name = (name.match(/^(\w[\w_$-]+)\.db$/) || [])[1]
        if (name)
          names.push(name)
      })
    } catch { }
    for (const col in this._collections)
      if (!names.includes(col))
        names.push(col)
    return names
  }

  async dropDatabase () {
    // drop all collections
    const promises = []
    for (const n in this._collections)
      promises.push(this._action(this._collections[n], 'drop'))
    this._collections = {}
    await Promise.all(promises)
    return this._adapter.dropDb()
  }
}

export class Client {
  constructor (url, options) {
    this._db = new MicroDB(url, options)
  }

  async open () {
    await this._db.close()
    return this._db.open({})
  }

  db (name) {
    return this._db
  }

  isConnected () {
    return this._db.isConnected
  }

  close () {
    return this._db.close()
  }
}
