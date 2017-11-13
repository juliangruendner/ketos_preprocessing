library("jsonlite", lib.loc="/usr/local/lib/R/3.4/site-library")
data = readLines("http://localhost:5000/aggregation")
x = fromJSON(data)
tmp = x$observations[[1]][[6]][,c("attribute", "value", "timestamp")]
#reshape(x$observations[[1]][[6]], idvar="attribute", direction = "wide")

dcast(setDT(tmp), 
      attribute ~ rowid(attribute, prefix = ""), 
      value.var = "value")

x[, attribute := list(c(1,2,3), c(3,4,5))]

lapply(x$attribute, mean)

dput(x, file="~/Downloads/x")
dget(file="~/Downloads/x")