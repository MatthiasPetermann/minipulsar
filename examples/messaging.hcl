security {
  mode = "open"
}

namespace "persistent://public/default" {
  produce = ["tester"]
  consume = ["tester"]
  subscription_timeout_seconds = 3600
  retention_seconds = 300
}

function "transform" {
  path = "transform.lua"
  max_runtime = "250ms"
}

binding {
  source = "persistent://public/default/temperature.f"
  function = "transform"
  target = "persistent://public/default/temperature.c"
}
