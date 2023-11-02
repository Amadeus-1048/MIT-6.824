package mr

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// 设计为原子操作，通过首先写入临时文件再替换原始文件，以避免在写入过程中发生的潜在错误或数据竞争
func atomicWriteFile(filename string, r io.Reader) (err error) {
	dir, file := filepath.Split(filename) // 根据最后一个路径分隔符将路径 path 分隔为目录和文件名两部分
	if dir == "" {                        // 如果没有指定目录（即目录为空），则默认使用当前目录（"."）
		dir = "."
	}
	//  创建临时文件
	f, err := os.CreateTemp(dir, file)
	if err != nil {
		return fmt.Errorf("cannot create temp file: %v", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(f.Name()) // 发生错误后要手动删除创建的临时文件夹
		}
	}()
	defer f.Close()
	name := f.Name()
	// 将数据写入到临时文件
	if _, err = io.Copy(f, r); err != nil { // 将数据从 io.Reader 复制到临时文件
		return fmt.Errorf("cannot write data to tempfile %q: %v", name, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("can't close tempfile %q: %v", name, err)
	}
	// 处理文件权限
	info, err := os.Stat(filename)
	if os.IsNotExist(err) { // 源文件不存在

	} else if err != nil {
		return err
	} else {
		if err = os.Chmod(name, info.Mode()); err != nil {
			return fmt.Errorf("can't set filemode on tempfile %q: %v", name, err)
		}
	}
	// 原子性替换原始文件
	if err = os.Rename(name, filename); err != nil { // 将临时文件重命名为目标文件名
		return fmt.Errorf("cannot replace %q with tempfile %q: %v", filename, name, err)
	}
	return err
}
