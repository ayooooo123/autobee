const test = require('brittle')
const crypto = require('hypercore-crypto')
const ID = require('hypercore-id-encoding')
const b4a = require('b4a')
const { create, replicate, replicateAndSync, sync, same, encode } = require('./helpers')

function randomWriters(n) {
  const keys = []
  for (let i = 0; i < n; i++) keys.push(crypto.keyPair().publicKey)
  return keys
}

async function length(auto, key) {
  const info = await auto.system.get(key)
  return info ? info.length : 0
}

test('untrusted-writers - an untrusted peer drops new writers past the limit', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key, { isTrusted: () => false, maxUntrustedWriters: 3 })
  const auto3 = await create(t, auto1.key)

  const keys = randomWriters(3)
  for (const key of keys) await auto1.append(encode({ addWriter: ID.encode(key) }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await replicateAndSync(auto1, auto2, auto3)

  t.ok(auto1.stats.writersOpened >= 4, 'the writer opened them all')
  t.ok(auto2.writers.size <= 3, 'never more than the budget tracked at once')

  for (const key of keys) {
    const info = await auto2.system.get(key)
    t.ok(info && !info.isRemoved && info.maxWeight > 0, 'the system still records the writer')
  }

  await auto3.append(encode({ msg: 'seen by the mirror' }))
  await replicateAndSync(auto1, auto2, auto3)

  t.is(await length(auto2, auto3.local.key), 1, 'the mirror follows the dropped writer')
})

test('untrusted-writers - a trusted peer opens past the limit', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key, { maxUntrustedWriters: 2 })

  const keys = randomWriters(3)
  for (const key of keys) await auto1.append(encode({ addWriter: ID.encode(key) }))
  await replicateAndSync(auto1, auto2)

  t.ok(auto2.stats.writersOpened >= 4, 'the limit does not apply to trusted peers')
  t.ok(auto2.stats.writersClosed <= auto2.stats.writersOpened, 'closes never exceed opens')

  await auto2.close()

  t.is(auto2.stats.writersClosed, auto2.stats.writersOpened, 'every opened writer is closed')
})

test('untrusted-writers - a stalled mirror fast-forwards once the admin moves on', async function (t) {
  const admin = await create(t, {
    mostRecentTrusted: () => ({ key: admin.local.key, length: admin.local.length })
  })

  const writers = []
  for (let i = 0; i < 4; i++) writers.push(await create(t, admin.key))

  for (const w of writers) await admin.append(encode({ addWriter: w.local.id }))
  await replicateAndSync(admin, ...writers)

  for (const w of writers) await w.append(encode({ msg: 'from ' + w.local.id }))
  await replicateAndSync(admin, ...writers)

  await admin.append(encode({ msg: 'links them all' }))

  // trust nothing at first so the mirror applies rather than fast-forwards
  let trustAdmin = false
  const mirror = await create(t, admin.key, {
    isTrusted: (key) => trustAdmin && b4a.equals(key, admin.local.key),
    maxUntrustedWriters: 3
  })

  const done = replicate(admin, mirror)

  for (let i = 0; i < 5; i++) {
    await mirror.update()
    await new Promise((resolve) => setTimeout(resolve, 50))
  }

  t.ok(mirror.writers.size <= 3, 'the mirror is at its limit')
  t.ok((await length(mirror, admin.local.key)) < admin.local.length, 'and stalled behind the admin')

  const moved = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('no fast-forward')), 10_000)
    mirror.once('move-to', () => {
      clearTimeout(timer)
      resolve()
    })
  })

  trustAdmin = true
  for (let i = 0; i < 40; i++) await admin.append(encode({ value: 'fix ' + i }))

  await t.execution(moved, 'the mirror fast-forwarded')
  await sync(admin, mirror)
  await done()

  t.ok(await same(admin, mirror), 'the mirror converged on the admin')
})

test('untrusted-writers - a removed writer is no longer trusted from cache', async function (t) {
  const owner = await create(t)
  const device = await create(t, owner.key)

  let revoked = false
  let asked = 0
  const follower = await create(t, owner.key, {
    isTrusted: (key) => {
      if (!b4a.equals(key, device.local.key)) return false
      asked++
      return !revoked
    }
  })

  await owner.append(encode({ addWriter: device.local.id }))
  await replicateAndSync(owner, device, follower)

  t.ok(await follower.trusted.isTrusted(device.local.key), 'trusted while granted')
  t.ok(await follower.trusted.isTrusted(device.local.key), 'answered again')
  t.is(asked, 1, 'the second answer came from the cache')

  revoked = true
  await owner.append(encode({ removeWriter: device.local.id }))
  await replicateAndSync(owner, follower)

  t.absent(
    await follower.trusted.isTrusted(device.local.key),
    'the removal drops the cached answer'
  )
  t.is(asked, 2, 'the hook was asked again')
})

test('untrusted-writers - a removal during a pending trust answer is not trusted', async function (t) {
  const owner = await create(t)
  const device = await create(t, owner.key)

  const answers = []
  let deferred = false
  const follower = await create(t, owner.key, {
    isTrusted: () => (deferred ? new Promise((resolve) => answers.push(resolve)) : false)
  })

  await owner.append(encode({ addWriter: device.local.id }))
  await replicateAndSync(owner, device, follower)
  deferred = true

  const first = follower.trusted.isTrusted(device.local.key)
  const shared = follower.trusted.isTrusted(device.local.key)
  t.is(answers.length, 1, 'one hook call in flight')

  follower.trusted.forget(device.local.key)
  answers[0](true)

  t.absent(await first, 'the in-flight answer is not trusted after the removal')
  t.absent(await shared, 'nor for a caller sharing it')

  const again = follower.trusted.isTrusted(device.local.key)
  t.is(answers.length, 2, 'nothing was cached: the hook is asked again')
  answers[1](false)
  t.absent(await again)
})
