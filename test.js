import assert from 'assert'
import fs from 'fs/promises'
import { MicroDB } from './microdb.js'

global.test = (name, fn) => {(test.tests ||= []).push({ name, fn })}
test.skip = (name) => {if (!name) test._skip = true; else (test.tests ||= []).push({ name, skip: true })}
test.run = async () => {
  let prefix = test._prefix || ''
  test._count ||= 0
  test._failed ||= 0
  for (const obj of test.tests) {
    test._count++
    const stime = performance.now()
    try {
      test.tests = []
      test._prefix = prefix + '  '
      if (!(test._skip = obj.skip))
        await obj.fn()
      if (test._skip) {
        console.log(`${prefix}\x1b[90m- ${obj.name} (skipped)\x1b[0m`)
        test._count--
        continue
      }
      console.log(`${prefix}\x1b[32m✔️ ${obj.name}\x1b[90m (${(performance.now() - stime).toFixed(0)}ms)\x1b[0m`)
      if (test.tests.length) {
        await test.run()
      }
    } catch (e) {
      test._failed++
      console.log(`${prefix}\x1b[31m❌ ${obj.name}: ${e.message}\x1b[90m (${(performance.now() - stime).toFixed(0)}ms)\x1b[0m`)
      if (e.name !== 'AssertionError')
        console.error(e.stack)
    }
  }
  if (!prefix)
    console.log(`${test._failed?'\x1b[33m':'\x1b[32m'}${'_'.repeat(16)}\n${test._failed?'❌':'✔️'} ${test._count-test._failed}/${test._count} DONE\x1b[0m`)
}
process.nextTick(test.run)

let db, col

test('Prepare', async () => {
  await fs.mkdir('tmp').catch(() => {})
  await fs.rm('tmp/col.db').catch(() => {})
  db = new MicroDB('microdb://tmp')
  col = await db.collection('col')
})

test('Content', async () => {
  await col.insertOne({_id:'000000000000000000000001', test: 'test'})
  await col.updateOne({_id:'000000000000000000000001'}, {test: 'test2'})
  await col.deleteOne({_id:'000000000000000000000001'})
  await db._action(col)
  assert.equal(await fs.readFile('./tmp/col.db', 'utf8'),
    JSON.stringify({_id:'000000000000000000000001', test: 'test'}) + '\n' +
    JSON.stringify({_id:'000000000000000000000001', test: 'test2'}) + '\n' +
    JSON.stringify({$$delete:'000000000000000000000001'}) + '\n')
})

test('Many', async () => {
  await col.insertMany([{_id:'000000000000000000000001', test: 'test'}, {_id:'000000000000000000000002', test: 'test2'}, {_id:'000000000000000000000003', test: 'test3'}])
  assert.deepEqual(await col.findOne({_id:'000000000000000000000001'}), {_id:'000000000000000000000001', test: 'test'})
  assert.deepEqual(await col.findOne({_id:'000000000000000000000003'}), {_id:'000000000000000000000003', test: 'test3'})
})

test('Update', async () => {
  await col.insertOne({_id:'000000000000000000000004', test: 'test4'})
  await col.updateMany({test:{$gt:'test2'}}, {test: 'test5'})
  assert.deepEqual(await col.findOne({_id:'000000000000000000000003'}), {_id:'000000000000000000000003', test: 'test5'})
  assert.deepEqual(await col.findOne({_id:'000000000000000000000004'}), {_id:'000000000000000000000004', test: 'test5'})
})

test('Count', async () => {
  assert.deepEqual(await col.countDocuments(), 4)
  assert.deepEqual(await col.countDocuments({test:{$eq:'test2'}}), 1)
  assert.deepEqual(await col.countDocuments({$or:[{test:{$eq:'test'}},{test:{$eq:'test5'}}]}), 3)
})

test('Reopen', async () => {
  await col.insertOne({_id:'100000000000000000000001', test: 'test'})
  await col.updateOne({_id:'100000000000000000000001'}, {test: 'test2'})
  await db.close()

  db = new MicroDB('microdb://tmp')
  col = await db.collection('col')
  assert.deepEqual(await col.findOne({_id:'100000000000000000000001'}), {_id:'100000000000000000000001', test: 'test2'})
})

test('Close', async () => {
  await db.close()
})

test('Cleanup', async () => {
  await fs.rm('./tmp', { recursive: true })
})
