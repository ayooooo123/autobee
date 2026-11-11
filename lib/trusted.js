const b4a = require('b4a')

class TrustedPeers {
  constructor(auto, hooks) {
    this.trusted = new Map()
    this.pending = new Map()
    this.auto = auto
    this.hooks = hooks
  }

  async isTrusted(key, reference) {
    const hex = b4a.toString(key, 'hex')
    if (this.trusted.get(hex)) return true

    // bootstrap
    if (this.auto.system.isGenesis() && b4a.equals(key, this.auto.key)) {
      return true
    }

    // legacy handling
    if (this.auto.system.indexers) {
      for (const idx of this.auto.system.indexers) {
        if (b4a.equals(key, idx.key)) return true
      }
    }

    const asking = this.pending.get(hex)
    if (asking) return asking.promise

    const token = {}
    const promise = this._ask(hex, key, reference, token)
    this.pending.set(hex, { promise, token })
    return promise
  }

  // the verdict holds only if nothing forgot this key while the hook ran: a writer removed (or an
  // undo) meanwhile must not be trusted by this answer, nor by callers sharing it
  async _ask(hex, key, reference, token) {
    const trusted = await (this.hooks.isTrusted
      ? this.hooks.isTrusted(key, reference)
      : !this.hooks.mostRecentTrusted)
    const asking = this.pending.get(hex)
    if (!asking || asking.token !== token) return false
    this.pending.delete(hex)
    if (trusted) this.trusted.set(hex, trusted)
    return trusted
  }

  read(trusted) {
    return trusted || null
  }

  mostRecentTrusted(target, reference) {
    if (!this.hooks.mostRecentTrusted) return Promise.resolve(null)
    return this.hooks.mostRecentTrusted(target, reference)
  }

  // a removed writer must be judged again: its earlier answer no longer holds
  forget(key) {
    const hex = b4a.toString(key, 'hex')
    this.pending.delete(hex)
    this.trusted.delete(hex)
  }

  clear() {
    this.pending.clear()
    this.trusted.clear()
  }
}

module.exports = TrustedPeers
