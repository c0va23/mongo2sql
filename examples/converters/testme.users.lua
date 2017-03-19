function printDoc(doc, prefix)
  if nil == prefix then
    prefix = ""
  end

  for key, value in pairs(doc) do
    fullKey = prefix .. "." .. key
    if "table" == type(value) then
      printDoc(value, fullKey)
    else
      print(fullKey, value)
    end
  end
end

function inserted(doc)
	print("insert")
	printDoc(doc)
  if doc.email then
    if exec("INSERT INTO users(id, email, name) VALUES($1, $2, $3)", doc._id, doc.email, doc.name) then
      print("inserted")
    end
  else
    print("skip")
  end
end

function updated(query, doc)
	print("update")
	printDoc(query)
	printDoc(doc)
end

function deleted(query)
	print("query")
	printDoc(query)
end
