local component = require("component")
local computer = require("computer")

if not component.isAvailable("eeprom") then
	error("EEPROM component is required for installation")
end

local handle, data, chunk = component.internet.request("https://raw.githubusercontent.com/CCCRSputnik/MineOS-2.0/master/Installer/BIOS.lua"), ""
   
while true do
	chunk = handle.read(math.huge)
	
	if chunk then
		data = data .. chunk
	else
		break
	end
end

handle.close()

local result, reason = load(data, "=installer")
if result then
	component.eeprom.set(data)
	computer.shutdown(true)
else
	error(reason)	
end