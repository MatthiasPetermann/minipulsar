function handle(payload, ctx)
  local value = tonumber(payload)
  if value == nil then
    return payload
  end
  local celsius = (value - 32) * 5 / 9
  return string.format("%.2f", celsius)
end
