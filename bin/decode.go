package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

const MbSize = 1048576
var (
	fileName string
	prefixLen int
)

type Decode struct {
	KeyDb string
	KeyType string
	KeyPrefix string
	IsExpire bool
}

type Size struct {
	TotalCount int
	TotalSize int
	MinSize int
	MaxSize int
}

type Pair struct {
	Decode
	Size
}

type Pairs []Pair

func (p Pairs)Len() int {
	return len(p)
}

func (p Pairs)Swap(i,j int)  {
	p[i],p[j] = p[j],p[i]
}

func (p Pairs)Less(i,j int) bool {
	pairA,pairB := p[i],p[j]

	if pairA.TotalSize > pairB.TotalSize{
		return true
	}else {
		return pairA.TotalCount > pairB.TotalCount
	}
}

func init()  {
	flag.StringVar(&fileName,"filename","","指定解析的文件名")
	flag.IntVar(&prefixLen,"prefix_len",8,"指定截取key前缀长度")
}


func main()  {
	var sortPairs Pairs
	var r io.Reader
	flag.Parse()
	decodeRes := make(map[Decode]Size)

	//fileName = "d:\\temp\\memory_demo.csv"
	// 从文件或者标准输入中读取待分析的数据
	if fileName != "" {
		file,err := os.Open(fileName)
		r = file
		if err !=nil{
			panic(err)
		}
		defer file.Close()
	}else {
		r = os.Stdin
	}

	reader := bufio.NewReader(r)
	for {
		str,err := reader.ReadString('\n')
		if err == io.EOF{
			break
		}
		if !strings.Contains(str,"database"){
			decodeRes = DecodeStr(str,prefixLen,decodeRes)
		}
	}
	// 装入sortPairs中
	for key,value := range decodeRes{
		sortPairs = append(sortPairs,Pair{key,value})
	}
	// 对其进行排序
	sort.Sort(sortPairs)
	for _,pair := range sortPairs {
		//fmt.Printf("type:%s,key_prefix:%s,is_expired:%t,totalCount:%d,totalSize:%d,minSize:%d,maxSize:%d\n",
		fmt.Printf("key前缀:%s,总大小:%fMb,总个数:%d,所在db:%s,类型:%s," +
			"是否会过期:%t,最小值:%d字节,最大值:%d字节\n",
			pair.KeyPrefix,float64(pair.TotalSize)/MbSize,pair.TotalCount,pair.KeyDb,pair.KeyType,
			pair.IsExpire,pair.MinSize,pair.MaxSize)
	}
}

func DecodeStr(s string,prefixLen int,totalDecs map[Decode]Size ) map[Decode]Size {
	var keyPrefix string
	res := strings.Split(s,",")
	KeyDb := res[0]
	keyType := res[1]
	keyName := res[2]
	if len(keyName) < prefixLen {
		keyPrefix = keyName[:]
	}else {
		keyPrefix = keyName[:prefixLen]
	}
	// 从后往前取索引,兼容如果keyName中含有,解析出多个字符时
	keySize,err := strconv.Atoi(res[len(res) - 5 ])
	if err !=nil{
		fmt.Printf("can't decode key %s\n",keyName)
	}
	newDec := Decode{
		KeyDb: 	KeyDb,
		KeyType:   keyType,
		KeyPrefix: keyPrefix,
		IsExpire:  !(res[7] == "\r\n" || res[7] == ""),
	}

	newSize := Size{
		TotalCount: 1,
		TotalSize: keySize,
		MinSize: keySize,
		MaxSize: keySize,
	}

	if value,ok := totalDecs[newDec];ok{
		value.TotalCount ++
		value.TotalSize += keySize
		if value.MaxSize < keySize{
			value.MaxSize = keySize
		}
		if value.MinSize > keySize{
			value.MinSize = keySize
		}
		totalDecs[newDec] = value
	}else {
		totalDecs[newDec] = newSize
	}
	return totalDecs
}

