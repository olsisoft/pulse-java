# Canonical Pulse local/private HTTP profile

Canonical producer: [olsisoft/pulse](https://github.com/olsisoft/pulse). The separate
`examples/kafka-inline-ai-stack/docker-compose.canonical-pulse.yml` consumes only an explicit locally built candidate image. It never pulls a
registry default, builds historical Source, or merges an old embedded/mesh world.
Existing compose files/images/versions remain unchanged LEGACY_COMPATIBILITY.

Supply `PULSE_LOCAL_IMAGE` from the new qualified PE1 candidate receipt, an explicit
private `PULSE_LOCAL_DATA_DIR`, and `PULSE_CUSTODY_MODE=fresh|adopt|open`. Fresh
requires a new empty directory. Adopt is only for historical H2 adoption through
the actual offline custody CLI. Open is ordinary restart of existing canonical
state: it requires the existing database and custody descriptor; runtime admission
validates their identity and roles without an offline CLI or rebase. There is no
automatic fallback between modes. Supply the installation identity for every mode.
Maintenance reference and explicit legacy-process quiescence/write-fencing flags
are required by fresh/adopt only; leave them unset for open. Supply full JDK 25
in the qualified producer image (existing H2 Java-source triggers require it),
and distinct owner/runtime roles. Mount the two agent owner/runtime password FILE inputs as private
regular files readable only by the image runtime identity. No raw password ENV is
passed to the child and missing paths are not auto-created. Do not log their values.
The qualified common database owner remains `sa` with an empty password; this
profile deliberately omits a common-owner password FILE input. A custom common
owner password is NOT_QUALIFIED by this profile.

Run `docker compose -f examples/kafka-inline-ai-stack/docker-compose.canonical-pulse.yml config` first. Local qualification may then start this
profile after the image and exact artifact subject are verified. This candidate
does not run production deployment or publish an image.

This profile provides standalone H2/LiteEventLog authenticated HTTP. It does not
claim the historical Kafka EMBEDDED/NATIVE/REMOTE bridge, mesh, HA or full market
pipeline has become qualified. Supported local checks use the canonical server's
public catalog/HTTP boundary; missing optional capability remains explicit rather
than restoring a private engine or changing template authority.

## Canonical guest SDK package for WASM examples

The three example Cargo manifests consume only the explicitly prepared
`.pulse-canonical-guest` inside this SDK checkout. They no longer inspect a
sibling `pulse-wasm-guest` or StreamFlow source tree. Obtain the local `.crate`
from the new qualified Pulse candidate and its HEAD/tree/SHA receipt, then run:

```sh
python tools/prepare-canonical-guest.py --crate /path/to/pulse-wasm-guest-0.1.0.crate \
  --sha256 <qualified-crate-sha256> --source-head <candidate-head> \
  --source-tree <candidate-tree> --output .pulse-canonical-guest
```

A missing or mismatched package fails explicitly; no opportunistic registry
package or historical source checkout is substituted. This is local package
consumption, not crate publication. These example WASM checks do not qualify
their old Kafka/mesh/HA deployment topology.
