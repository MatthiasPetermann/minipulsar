security {
  mode = "strict"
}

namespace "persistent://public/default" {
  produce = ["tester"]
  consume = ["tester"]
}

function "transform" {
  path = "transform.lua"
}

binding {
  source = "persistent://public/default/temperature.f"
  function = "transform"
  target = "persistent://public/default/temperature.c"
}
