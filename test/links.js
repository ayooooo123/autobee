const test = require('brittle')
const Corestore = require('corestore')
const { create, replicate, replicateAndSync, sync, encode, same } = require('./helpers')

test('links - writer sees previous writes via links', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await replicateAndSync(auto1, auto2)

  // auto1 writes first
  await auto1.append(encode({ msg: 'first' }))
  await replicateAndSync(auto1, auto2)

  // auto2 writes after seeing auto1's write — its oplog entry will link to auto1's
  await auto2.append(encode({ msg: 'second, after seeing first' }))
  await replicateAndSync(auto1, auto2)

  t.ok(await same(auto1, auto2), 'peers converge')
})

test('links - dependency arrives before dependent', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await replicateAndSync(auto1, auto2)

  // auto1 writes
  await auto1.append(encode({ msg: 'dependency' }))
  await replicateAndSync(auto1, auto2)

  // auto2 writes after seeing auto1's write (creates a link)
  await auto2.append(encode({ msg: 'dependent' }))

  // Sync — auto1 already has the dependency, so auto2's entry should process immediately
  await replicateAndSync(auto1, auto2)

  t.ok(await same(auto1, auto2), 'peers converge when dependency arrives first')
})

test('links - dependent arrives before dependency', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)
  const auto3 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await replicateAndSync(auto1, auto2, auto3)

  // auto1 writes
  await auto1.append(encode({ msg: 'step1' }))
  await replicateAndSync(auto1, auto2)

  // auto2 writes after seeing auto1's step1 (links to it)
  await auto2.append(encode({ msg: 'step2-links-to-step1' }))

  // Sync auto2 → auto3 first (auto3 gets auto2's entry but NOT auto1's step1 yet)
  await replicateAndSync(auto2, auto3)

  // Now sync auto1 → auto3 (auto3 gets the dependency)
  await replicateAndSync(auto1, auto3)

  // Final sync to converge
  await replicateAndSync(auto1, auto2, auto3)

  t.ok(await same(auto1, auto2, auto3), 'peers converge when dependent arrives before dependency')
})

test('links - causal chain across three writers', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)
  const auto3 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await replicateAndSync(auto1, auto2, auto3)

  // Chain: auto1 writes → auto2 sees it and writes → auto3 sees auto2's and writes
  await auto1.append(encode({ msg: 'chain-1' }))
  await replicateAndSync(auto1, auto2)

  await auto2.append(encode({ msg: 'chain-2' }))
  await replicateAndSync(auto2, auto3)

  await auto3.append(encode({ msg: 'chain-3' }))

  // Sync everyone
  await replicateAndSync(auto1, auto2, auto3)

  t.ok(await same(auto1, auto2, auto3), 'peers converge with causal chain')
})

// Previously hung (non-productive undo requeue loop), then diverged (concurrent
// equal-weight entries placed differently per peer due to insert-time register
// sampling). Both fixed: addSorted index/undo shared-prefix detection ends the loop,
// per-node immutable weight witnesses (lib/witness.js) make the placement deterministic.
test('links - multiple links from one entry', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)
  const auto3 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await replicateAndSync(auto1, auto2, auto3)
  t.ok(await same(auto1, auto2, auto3), 'peers converge with multiple links')

  // auto2 and auto3 both write concurrently
  await auto2.append(encode({ msg: 'from-auto2' }))
  await auto3.append(encode({ msg: 'from-auto3' }))

  // Sync so auto1 sees both
  await replicateAndSync(auto1, auto2, auto3)

  // auto1 writes after seeing both — links to both auto2 and auto3's entries
  await auto1.append(encode({ msg: 'after-both' }))
  await replicateAndSync(auto1, auto2, auto3)

  t.ok(await same(auto1, auto2, auto3), 'peers converge with multiple links')
})

test('links - no links on first write', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await replicateAndSync(auto1, auto2)

  // Both write concurrently — neither has seen the other, so no cross-links
  await auto1.append(encode({ msg: 'auto1-independent' }))
  await auto2.append(encode({ msg: 'auto2-independent' }))

  await replicateAndSync(auto1, auto2)

  t.ok(await same(auto1, auto2), 'peers converge with independent writes')
})

test('links - reverse order arrival of dependency chain', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)
  const auto3 = await create(t, auto1.key)
  const auto4 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await auto1.append(encode({ addWriter: auto4.local.id }))
  await replicateAndSync(auto1, auto2, auto3, auto4)

  // Build a chain: auto2 writes → auto3 sees auto2 and writes → auto4 sees auto3 and writes
  await auto2.append(encode({ msg: 'chain-start' }))
  await replicateAndSync(auto2, auto3)

  await auto3.append(encode({ msg: 'chain-middle' }))
  await replicateAndSync(auto3, auto4)

  await auto4.append(encode({ msg: 'chain-end' }))

  // Now sync to auto1 in REVERSE order: auto4 first, then auto3, then auto2
  // auto1 gets the dependent before its dependencies
  await replicateAndSync(auto1, auto4)
  await replicateAndSync(auto1, auto3)
  await replicateAndSync(auto1, auto2)

  // Final sync to converge
  await replicateAndSync(auto1, auto2, auto3, auto4)

  t.ok(
    await same(auto1, auto2, auto3, auto4),
    'all peers converge after reverse-order chain arrival'
  )
})

// Previously hung (non-productive undo requeue loop on the diamond-tip node linking
// both branches). Fixed by the addSorted index/undo shared-prefix detection + per-node
// immutable weight witnesses (lib/witness.js).
test('links - diamond dependency pattern', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)
  const auto3 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await replicateAndSync(auto1, auto2, auto3)

  // auto1 writes the base
  await auto1.append(encode({ msg: 'base' }))
  await replicateAndSync(auto1, auto2, auto3)

  // auto2 and auto3 both see auto1's write and write concurrently (both link to base)
  await auto2.append(encode({ msg: 'left-branch' }))
  await auto3.append(encode({ msg: 'right-branch' }))

  // Sync so auto1 sees both branches
  await replicateAndSync(auto1, auto2, auto3)

  // auto1 writes after seeing both branches (links to both — diamond tip)
  await auto1.append(encode({ msg: 'diamond-tip' }))
  await replicateAndSync(auto1, auto2, auto3)

  t.ok(await same(auto1, auto2, auto3), 'peers converge with diamond dependency')
})

test('links - trigger cascade with multiple waiting writers', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)
  const auto3 = await create(t, auto1.key)
  const auto4 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await auto1.append(encode({ addWriter: auto4.local.id }))
  await replicateAndSync(auto1, auto2, auto3, auto4)

  // auto2 writes the root
  await auto2.append(encode({ msg: 'root' }))
  await replicateAndSync(auto2, auto3)
  await replicateAndSync(auto2, auto4)

  // auto3 and auto4 BOTH link to auto2's write independently
  await auto3.append(encode({ msg: 'depends-on-root-A' }))
  await auto4.append(encode({ msg: 'depends-on-root-B' }))

  // Sync auto3 and auto4 to auto1 BEFORE auto2
  // Both will be waiting on auto2's entry via triggers
  await replicateAndSync(auto1, auto3)
  await replicateAndSync(auto1, auto4)

  // Now sync auto2 — should trigger both auto3 and auto4's entries to process
  await replicateAndSync(auto1, auto2)

  // Final convergence
  await replicateAndSync(auto1, auto2, auto3, auto4)

  t.ok(await same(auto1, auto2, auto3, auto4), 'all peers converge after trigger cascade')
})

test('links - writer removed while others link to its entries', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)
  const auto3 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await auto1.append(encode({ addWriter: auto3.local.id }))
  await replicateAndSync(auto1, auto2, auto3)

  // auto2 writes
  await auto2.append(encode({ msg: 'from-auto2' }))
  await replicateAndSync(auto1, auto2, auto3)

  // auto3 sees auto2's write and writes (links to auto2's entry)
  await auto3.append(encode({ msg: 'links-to-auto2' }))

  // auto1 removes auto2 concurrently
  await auto1.append(encode({ removeWriter: auto2.local.id }))

  // Sync everyone
  await replicateAndSync(auto1, auto2, auto3)

  // auto3's write should still be processed even though auto2 is now removed
  // because auto2's entry was already indexed before removal
  const info2 = await auto1.system.get(auto2.local.key)
  t.ok(info2.isRemoved, 'auto2 is removed')
  t.ok(auto3.writable, 'auto3 is still writable')
  t.ok(await same(auto1, auto3), 'auto1 and auto3 converge')
})

test('links - deep sequential chain', async function (t) {
  const auto1 = await create(t)
  const auto2 = await create(t, auto1.key)

  await auto1.append(encode({ addWriter: auto2.local.id }))
  await replicateAndSync(auto1, auto2)

  // Alternating writes with syncs — each entry links to the previous
  for (let i = 0; i < 5; i++) {
    const writer = i % 2 === 0 ? auto1 : auto2
    await writer.append(encode({ msg: 'step-' + i }))
    await replicateAndSync(auto1, auto2)
  }

  t.ok(await same(auto1, auto2), 'peers converge after deep sequential chain')
})

// a follower that applied later-stamped nodes before meeting a chain of earlier
// ones, each unblocked by the one before, re-applied that suffix once per chain
// node: a reapply storm quadratic in the chain length (and a fresh join of a
// busy base is nothing but such chains)
test('links - a chain of late nodes re-applies the suffix once', async function (t) {
  const SUFFIX = 20
  const CHAIN = 10
  const SKEW = 30 * 60 * 1000

  // a reorder re-reads the undone nodes, so a fast-forwarded follower would
  // wait on peers for the blocks it skipped
  const root = await create(t, { fastForward: false })
  const a = await create(t, root.key, { fastForward: false })
  const b = await create(t, root.key, { fastForward: false })
  // stamped half an hour ahead, so everything c writes sorts after the chain
  const c = await create(t, root.key, { fastForward: false, now: () => Date.now() + SKEW })

  await root.append(encode({ addWriter: a.local.id }))
  await root.append(encode({ addWriter: b.local.id }))
  await root.append(encode({ addWriter: c.local.id }))
  await replicateAndSync(root, a, b, c)

  for (let i = 0; i < SUFFIX; i++) await c.append(encode({ suffix: i }))

  // a and b ack each other: every node links the previous one
  const unreplicate = replicate(a, b)
  for (let i = 0; i < CHAIN; i++) {
    await a.append(encode({ ack: i, from: 'a' }))
    await sync(a, b)
    await b.append(encode({ ack: i, from: 'b' }))
    await sync(a, b)
  }
  await unreplicate()

  // the follower applies the suffix first and then meets the whole chain at
  // once: fetched into its store while closed, so no block trickles into a drain
  const storage = await t.tmp()
  const follower = await create(t, root.key, { storage, fastForward: false })
  await replicateAndSync(root, follower)
  await replicateAndSync(c, follower)
  await follower.close()

  const store = new Corestore(storage, { manifestVersion: 2 })
  for (const w of [a, b]) {
    const core = store.get(w.local.key)
    await core.ready()
    const unreplicate = replicate(w.store, store)
    await core.download({ start: 0, end: w.local.length }).done()
    await unreplicate()
  }
  await store.close()

  // the cores that changed while closed are hinted before the first drain
  const reopened = await create(t, root.key, { storage, fastForward: false })
  await reopened.update()

  t.is(reopened.stats.undos, 1, 'the suffix is undone once')
  t.is(reopened.stats.applies, 2 * CHAIN + SUFFIX, 'the suffix is re-applied once')

  await replicateAndSync(root, a, b, c, reopened)
  t.ok(await same(root, a, b, c, reopened), 'the follower converges with the writers')
  t.alike(reopened.system.hash, c.system.hash, 'and lands on the same system')
})
