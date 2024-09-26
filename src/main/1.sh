go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-intermidiate/mr-*
rm mr-out*
go run -race mrcoordinator.go pg-1*.txt

