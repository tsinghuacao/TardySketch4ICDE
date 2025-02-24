import xxhash

class XXHasher:
    """
    XXHash 哈希工具类
    支持32位和64位哈希生成
    """
    
    @staticmethod
    def hash32(data, seed=0):
        """生成32位哈希值"""
        if not isinstance(data, (bytes, str)):
            data = str(data)
        return xxhash.xxh32(data, seed=seed).intdigest()

    @staticmethod
    def hash64(data, seed=0):
        """生成64位哈希值"""
        if not isinstance(data, (bytes, str)):
            data = str(data)
        return xxhash.xxh64(data, seed=seed).intdigest()

    @staticmethod
    def hash_normalized(data, seed=0, bits=64):
        """生成标准化哈希值（0-1范围）"""
        if bits == 32:
            max_val = 0xFFFFFFFF
            hash_int = XXHasher.hash32(data, seed)
        else:
            max_val = 0xFFFFFFFFFFFFFFFF
            hash_int = XXHasher.hash64(data, seed)
        return hash_int / max_val

if __name__ == "__main__":
    data = "hello world"
    print(f"32-bit hash: {XXHasher.hash32(data)}")
    print(f"64-bit hash: {XXHasher.hash64(data)}")
    print(f"Normalized 64-bit: {XXHasher.hash_normalized(data)}")