# Messaging demo

This folder contains a sample messaging control-plane configuration and Lua function.

## Run

From this directory, start the broker with the sample config:

```bash
cd examples
../minipulsar --messaging-config=messaging.hcl
```

The config expects producers and consumers authenticated with the `tester` role and
routes `persistent://public/default/temperature.f` through the Lua function to
`persistent://public/default/temperature.c`.
