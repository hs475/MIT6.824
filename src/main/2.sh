go run -race mrsequential.go wc.so pg-1*.txt
cat mrout/mr-out-* | sort | more