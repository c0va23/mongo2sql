function inserted(doc)
	print("insert")
  if exec("INSERT INTO orders(price, good_id) VALUES($1, $2)", doc.price, doc.good_id) then
    print("inserted")
  end
end

function updated(query, doc)
	print("update skipped")
end

function deleted(query)
	print("query skipped")
end
