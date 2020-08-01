local addonName, addonTable = ...

SLASH_PRICERTEST1 = '/pricer'
function SlashCmdList.PRICERTEST(msg, editbox) -- 4.
 for item, val in pairs(addonTable.items) do
  o={}
  TUJMarketInfo(item, o)

  if o['market'] then
	  addonTable.items[item]['market'] = o['market']
	  addonTable.items[item]['recent'] = o['recent']
	  addonTable.items[item]['stddev'] = o['stddev']
    addonTable.items[item]['timestamp'] = o['age']
  else
  	print('Missing info for', item)
  end
 end

 _G["PricerData"] = addonTable.items
 print('Pricer parsed successfully')

end

SLASH_PRICERTESTVAL1 = '/pricerval'
function SlashCmdList.PRICERTESTVAL(msg, editbox) -- 4.
  o={}
  TUJMarketInfo(msg, o)
  print(o['market'], o['recent'], o['stddev'], o['age'])
end
