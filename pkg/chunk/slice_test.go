package chunk

import (
	"os"
	"testing"
	"time"

	"github.com/JustACP/criceta/pkg/common/idgen"
	"github.com/stretchr/testify/assert"
)

func initTestSlicePath() {
	SlicePath = "./slice_test"
}

func TestSliceHeader_ParseSerialize(t *testing.T) {

	initTestSlicePath()
	t.Parallel()

	// 准备测试数据
	fixedTime := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	original := &SliceHeader{
		Id:       1,
		ChunkId:  2,
		Version:  3,
		Size:     1024,
		Offset:   2048,
		Range:    SliceRange{Offset: 0, Size: 512},
		Status:   SLICE_VALID,
		CreateAt: fixedTime,
		ModifyAt: fixedTime,
	}

	// 测试序列化
	data := original.serializeSliceHeader()
	assert.Equal(t, SliceHeaderSize, len(data), "序列化后的数据长度不正确")

	// 测试解析
	parsed := &SliceHeader{}
	err := parsed.parseSliceHeader(data)
	assert.NoError(t, err, "解析失败")
	assert.Equal(t, original, parsed, "解析后的数据不匹配")

	// 测试错误情况
	shortData := data[:SliceHeaderSize-1]
	err = parsed.parseSliceHeader(shortData)
	assert.Error(t, err, "短数据应该返回错误")

	invalidMagic := make([]byte, SliceHeaderSize)
	copy(invalidMagic, data)
	invalidMagic[0] = 'X'
	err = parsed.parseSliceHeader(invalidMagic)
	assert.Error(t, err, "无效的 magic number 应该返回错误")
}

func TestNewSlice(t *testing.T) {
	initTestSlicePath()

	t.Parallel()

	// 准备测试数据
	sliceId := idgen.NextID()
	chunkId := uint64(1)
	version := uint64(1)
	offset := uint64(0)

	// 创建新 Slice
	slice, err := NewSlice(sliceId, chunkId, version, offset)
	assert.NoError(t, err, "创建 Slice 失败")
	defer os.Remove(slice.Path)
	defer slice.File.Close()

	// 验证 Header 信息
	assert.Equal(t, sliceId, slice.Header.Id, "Slice ID 不匹配")
	assert.Equal(t, chunkId, slice.Header.ChunkId, "Chunk ID 不匹配")
	assert.Equal(t, version, slice.Header.Version, "Version 不匹配")
	assert.Equal(t, offset, slice.Header.Offset, "Offset 不匹配")
	assert.Equal(t, SLICE_VALID, slice.Header.Status, "Status 不匹配")

	// 测试文件创建
	_, err = os.Stat(slice.Path)
	assert.NoError(t, err, "Slice 文件未创建")

	// 测试重复创建
	_, err = NewSlice(sliceId, chunkId, version, offset)
	assert.Error(t, err, "重复创建应该失败")
}

func TestSlice_WriteReadData(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 准备测试数据
	sliceId := idgen.NextID()
	chunkId := uint64(1)
	version := uint64(1)
	offset := uint64(0)

	slice, err := NewSlice(sliceId, chunkId, version, offset)
	assert.NoError(t, err, "创建 Slice 失败")
	defer os.Remove(slice.Path)
	defer slice.File.Close()

	// 测试写入数据
	testData := []byte("test data")
	err = slice.WriteData(testData)
	assert.NoError(t, err, "写入数据失败")

	// 验证 Header 更新
	assert.Equal(t, uint64(len(testData)), slice.DataSize, "DataSize 未更新")
	assert.Equal(t, uint64(len(testData)), slice.Header.Size, "Header Size 未更新")

	// 测试读取数据
	readData, err := slice.ReadData(0, uint64(len(testData)))
	assert.NoError(t, err, "读取数据失败")
	assert.Equal(t, testData, readData, "读取的数据不匹配")

	// 测试读取超出范围
	_, err = slice.ReadData(0, uint64(len(testData))+1)
	assert.Error(t, err, "读取超出范围应该失败")
}

func TestSliceManager_CreateAppendSlice(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建 SliceManager
	chunkId := uint64(1)
	maxSize := uint64(1024 * 1024)
	sm := NewSliceManager(chunkId, maxSize)

	// 创建新 Slice
	offset := uint64(0)
	slice, err := sm.CreateNewSlice(offset)
	assert.NoError(t, err, "创建新 Slice 失败")
	defer os.Remove(slice.Path)
	defer slice.File.Close()

	// 验证 Slice 信息
	assert.Equal(t, chunkId, slice.Header.ChunkId, "Chunk ID 不匹配")
	assert.Equal(t, uint64(1), slice.Header.Version, "Version 不匹配")

	// 测试追加 Slice
	err = sm.AppendSlice(slice)
	assert.NoError(t, err, "追加 Slice 失败")

	// 测试追加无效 Slice
	invalidSlice := &Slice{
		Header: &SliceHeader{
			ChunkId: chunkId + 1, // 不同的 Chunk ID
			Status:  SLICE_VALID,
		},
	}
	err = sm.AppendSlice(invalidSlice)
	assert.Error(t, err, "追加无效 Slice 应该失败")
}

func TestSliceManager_UpdateMapping(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建 SliceManager
	chunkId := uint64(1)
	maxSize := uint64(1024 * 1024)
	sm := NewSliceManager(chunkId, maxSize)

	// 创建多个 Slice
	slice1, _ := sm.CreateNewSlice(0)
	randData := make([]byte, 1024)
	for i := 0; i < 1024; i++ {
		randData[i] = 1
	}
	err := slice1.WriteData(randData)
	assert.NoError(t, err, "write slice data error")
	defer os.Remove(slice1.Path)
	defer slice1.File.Close()

	slice2, _ := sm.CreateNewSlice(1024)
	err = slice2.WriteData(randData)
	assert.NoError(t, err, "write slice data error")
	defer os.Remove(slice2.Path)
	defer slice2.File.Close()

	slice3, _ := sm.CreateNewSlice(2048)
	err = slice3.WriteData(randData)
	assert.NoError(t, err, "write slice data error")
	defer os.Remove(slice3.Path)
	defer slice3.File.Close()

	// 更新映射
	sm.UpdateMapping()

	// 验证映射
	expectedMapping := [][]*MappingSlice{
		{
			&MappingSlice{
				Range: SliceRange{Offset: 0, Size: 1024},
				Slice: slice1,
			},
			&MappingSlice{
				Range: SliceRange{Offset: 1024, Size: 1024},
				Slice: slice2,
			},
			&MappingSlice{
				Range: SliceRange{Offset: 2048, Size: 1024},
				Slice: slice3,
			},
		},
	}

	assert.Equal(t, expectedMapping, sm.mapping, "映射不匹配")
}

func TestMergeMapping_NoOverlap(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建多个 Slice
	slice1 := &Slice{
		Header: &SliceHeader{
			Offset: 0,
			Size:   1024,
		},
	}

	slice2 := &Slice{
		Header: &SliceHeader{
			Offset: 2048,
			Size:   1024,
		},
	}

	slice3 := &Slice{
		Header: &SliceHeader{
			Offset: 4096,
			Size:   1024,
		},
	}

	// 合并映射
	activeSlices := []*Slice{slice1, slice2, slice3}
	mapping := mergeMapping(activeSlices)

	// 验证映射
	expectedMapping := [][]*MappingSlice{
		{
			&MappingSlice{
				Range: SliceRange{Offset: 0, Size: 1024},
				Slice: slice1,
			},
		},
		{
			&MappingSlice{
				Range: SliceRange{Offset: 2048, Size: 1024},
				Slice: slice2,
			},
		},
		{
			&MappingSlice{
				Range: SliceRange{Offset: 4096, Size: 1024},
				Slice: slice3,
			},
		},
	}

	assert.Equal(t, expectedMapping, mapping, "映射不匹配")
}

func TestMergeMapping_Overlap(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建多个 Slice
	slice1 := &Slice{
		Header: &SliceHeader{
			Offset:  0,
			Size:    2048,
			Version: 10,
		},
	}

	slice2 := &Slice{
		Header: &SliceHeader{
			Offset:  1024,
			Size:    1024,
			Version: 9,
		},
	}

	slice3 := &Slice{
		Header: &SliceHeader{
			Offset:  1024,
			Size:    1025,
			Version: 7,
		},
	}

	slice4 := &Slice{
		Header: &SliceHeader{
			Offset:  3072,
			Size:    1024,
			Version: 1,
		},
	}

	// 合并映射
	activeSlices := []*Slice{slice1, slice2, slice3, slice4}
	mapping := mergeMapping(activeSlices)

	// 验证映射
	expectedMapping := [][]*MappingSlice{
		{
			&MappingSlice{
				Range: SliceRange{Offset: 0, Size: 2048},
				Slice: slice1,
			},
			&MappingSlice{
				Range: SliceRange{Offset: 1024, Size: 1025},
				Slice: slice3,
			},
		},
		{
			&MappingSlice{
				Range: SliceRange{Offset: 3072, Size: 1024},
				Slice: slice4,
			},
		},
	}

	assert.Equal(t, expectedMapping, mapping, "映射不匹配")
}

func TestSliceManager_MergeSlices(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建 SliceManager
	chunkId := uint64(1)
	maxSize := uint64(1024 * 1024)
	sm := NewSliceManager(chunkId, maxSize)

	// 创建多个 Slice 并写入数据
	slice1, _ := sm.CreateNewSlice(0)
	defer os.Remove(slice1.Path)
	defer slice1.File.Close()

	testData1 := []byte("test data 1")
	err := slice1.WriteData(testData1)
	assert.NoError(t, err, "写入数据到 slice1 失败")

	// 重叠一下
	slice2, _ := sm.CreateNewSlice(uint64(4))
	defer os.Remove(slice2.Path)
	defer slice2.File.Close()

	testData2 := []byte("test data 2")
	err = slice2.WriteData(testData2)
	assert.NoError(t, err, "写入数据到 slice2 失败")

	// 合并 Slice
	mergedSlice, err := sm.MergeSlices([]*Slice{slice1, slice2})
	assert.NoError(t, err, "合并 Slice 失败")
	defer os.Remove(mergedSlice.Path)
	defer mergedSlice.File.Close()

	// 验证合并后的 Slice 内容
	expectedData := append(testData1[0:4], testData2...)
	actualData, err := mergedSlice.ReadData(0, uint64(len(expectedData)))
	assert.NoError(t, err, "读取合并后的 Slice 数据失败")
	assert.Equal(t, expectedData, actualData, "合并后的 Slice 数据不匹配")
}

func TestSliceManager_WriteAt(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建 SliceManager
	chunkId := uint64(1)
	maxSize := uint64(1024 * 1024)
	sm := NewSliceManager(chunkId, maxSize)

	// 测试 WriteAt
	testData := []byte("test data")
	n, err := sm.WriteAt(testData, 0)
	assert.NoError(t, err, "WriteAt 失败")
	assert.Equal(t, len(testData), n, "写入的数据长度不匹配")

	// 验证数据
	readData := make([]byte, len(testData))
	_, err = sm.ReadAt(readData, 0)
	assert.NoError(t, err, "ReadAt 失败")
	assert.Equal(t, testData, readData, "数据不匹配")

	err = sm.deleteSlices(sm.Slices)
	assert.NoError(t, err, "删除Slice失败")
}

func TestSliceManager_Write(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建 SliceManager
	chunkId := uint64(1)
	maxSize := uint64(1024 * 1024)
	sm := NewSliceManager(chunkId, maxSize)

	// 测试 Write
	testData := []byte("test data")
	n, err := sm.Write(testData)
	assert.NoError(t, err, "Write 失败")
	assert.Equal(t, len(testData), n, "写入的数据长度不匹配")

	// 验证数据
	readData := make([]byte, len(testData))
	n, err = sm.Read(readData)
	assert.NoError(t, err, "Read 失败")
	assert.Equal(t, len(testData), n, "读取的数据长度不匹配")
	assert.Equal(t, testData, readData, "数据不匹配")

	err = sm.deleteSlices(sm.Slices)
	assert.NoError(t, err, "删除Slice失败")
}

func TestSliceManager_ReadAt(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建 SliceManager
	chunkId := uint64(1)
	maxSize := uint64(1024 * 1024)
	sm := NewSliceManager(chunkId, maxSize)

	// 写入数据
	testData := []byte("test data")
	n, err := sm.WriteAt(testData, 0)
	assert.NoError(t, err, "WriteAt 失败")
	assert.Equal(t, len(testData), n, "写入的数据长度不匹配")

	// 测试 ReadAt
	readData := make([]byte, len(testData))
	n, err = sm.ReadAt(readData, 0)
	assert.NoError(t, err, "ReadAt 失败")
	assert.Equal(t, len(testData), n, "读取的数据长度不匹配")
	assert.Equal(t, testData, readData, "数据不匹配")

	err = sm.deleteSlices(sm.Slices)
	assert.NoError(t, err, "删除Slice失败")
}

func TestSliceManager_Read(t *testing.T) {
	initTestSlicePath()
	t.Parallel()

	// 创建 SliceManager
	chunkId := uint64(1)
	maxSize := uint64(1024 * 1024)
	sm := NewSliceManager(chunkId, maxSize)

	// 创建新 Slice
	offset := uint64(0)
	slice, err := sm.CreateNewSlice(offset)
	assert.NoError(t, err, "创建新 Slice 失败")
	defer os.Remove(slice.Path)
	defer slice.File.Close()

	// 写入数据
	testData := []byte("test data")
	n, err := sm.WriteAt(testData, 0)
	assert.NoError(t, err, "WriteAt 失败")
	assert.Equal(t, len(testData), n, "写入的数据长度不匹配")

	// 测试 Read
	readData := make([]byte, len(testData))
	n, err = sm.Read(readData)
	assert.NoError(t, err, "Read 失败")
	assert.Equal(t, len(testData), n, "读取的数据长度不匹配")
	assert.Equal(t, testData, readData, "数据不匹配")

	err = sm.deleteSlices(sm.Slices)
	assert.NoError(t, err, "删除Slice失败")
}
