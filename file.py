import os
import base64
import random
import time
from db import FileDb
import hashlib

def sha256(data : str | bytes) -> str:
    if isinstance(data, str):
        data = bytes(data, encoding="utf-8")

    sha256_hash = hashlib.sha256()
    sha256_hash.update(data)

    return sha256_hash.hexdigest()

def init(port_api : int):
    if not os.path.exists("res/{}/file".format(port_api)):
        os.makedirs("res/{}/file".format(port_api))
    forum_cursor = FileDb("res/{}/file/file.db".format(port_api), port_api)
    forum_cursor.create_file_db()

def remove_outdate (port_api : int, file_cursor : FileDb): # 删除过期文件
    qry = file_cursor.lose_effect()
    for tmp in qry:
        if os.path.isfile("res/{}/file/{}.file".format(port_api, tmp[3])):
            os.remove("res/{}/file/{}.file".format(port_api, tmp[3]))

def upload_file(port_api : int, uid : int, file_b64 : str, file_name : str, file_cursor : FileDb):
    hashes = sha256(str(time.time()) + str(uid) + file_name)
    with open("res/{}/file/{}.file".format(port_api, hashes), "wb") as file:
        file.write(base64.b64decode(file_b64))
    file_cursor.tag_file(uid, file_name, time.time(), hashes)
    remove_outdate(port_api, file_cursor)
    return hashes


# 实验性新特性：流式分块上传接口，支持超大文件上传。客户端将文件分割成多个块，每次上传一个块，并在最后一个块上传完成后进行文件合并和完整性校验。
def chunked_upload_file(port_api : int, uid : int, file_name : str, chunk_index : int, chunk_total : int, chunk_data_b64 : str, file_id : str = None, file_cursor : FileDb = None, expected_hash : str = None):
    """
    流式分块上传文件（避免大文件内存溢出）
    
    :param port_api: API 端口
    :param uid: 用户 ID
    :param file_name: 文件名
    :param chunk_index: 当前块索引（从 0 开始）
    :param chunk_total: 总块数
    :param chunk_data_b64: 当前块的 Base64 编码数据
    :param file_id: 文件 ID（第一块时为 None，之后返回的值）
    :param file_cursor: 数据库游标（完成时需要）
    :param expected_hash: 客户端计算的文件哈希值（用于完整性校验）
    :return: dict 包含 success/error，以及 file_id（中间块）或 file_hash（最后一块）
    """
    # 单块大小限制（10MB）
    MAX_CHUNK_SIZE = 10 * 1024 * 1024
    # 单文件总大小上限（200MB，按最大块大小计算）
    MAX_TOTAL_SIZE = 200 * 1024 * 1024

    if not isinstance(chunk_index, int) or not isinstance(chunk_total, int) or chunk_index < 0 or chunk_total < 1 or chunk_index >= chunk_total:
        return {"success": False, "error": "Invalid chunk parameters"}

    if chunk_total * MAX_CHUNK_SIZE > MAX_TOTAL_SIZE:
        return {"success": False, "error": "File too large"}

    try:
        decoded_chunk = base64.b64decode(chunk_data_b64)
    except Exception as e:
        return {"success": False, "error": "Decode failed"}

    if len(decoded_chunk) > MAX_CHUNK_SIZE:
        return {"success": False, "error": "Chunk too large"}

    if chunk_index == 0:
        tmp_dir = "res/{}/file/".format(port_api)
        if os.path.isdir(tmp_dir):
            for fname in os.listdir(tmp_dir):
                if fname.startswith(".tmp_{}_".format(uid)):
                    try:
                        fpath = os.path.join(tmp_dir, fname)
                        if time.time() - os.path.getmtime(fpath) > 3600:
                            os.remove(fpath)
                    except OSError:
                        pass
            existing_ids = set()
            for fname in os.listdir(tmp_dir):
                if fname.startswith(".tmp_{}_".format(uid)):
                    parts = fname.split("_")
                    if len(parts) >= 4:
                        existing_ids.add(parts[2])
            if len(existing_ids) >= 5:
                return {"success": False, "error": "Too many concurrent uploads"}

    # 第一块：生成文件 ID 并清理旧 chunk 文件
    if chunk_index == 0:
        file_id = sha256(str(time.time()) + str(uid) + file_name)
        chunk_dir = "res/{}/file/".format(port_api)
        prefix = ".tmp_{}_{}_".format(uid, file_id)
        if os.path.isdir(chunk_dir):
            for fname in os.listdir(chunk_dir):
                if fname.startswith(prefix):
                    try:
                        os.remove(os.path.join(chunk_dir, fname))
                    except OSError:
                        pass
        try:
            if not os.path.exists(chunk_dir):
                os.makedirs(chunk_dir)
        except Exception as e:
            return {"success": False, "error": "Directory creation failed"}
        total_path_tmp = os.path.join(chunk_dir, ".tmp_{}_{}_total".format(uid, file_id))
        try:
            with open(total_path_tmp, "w") as tf:
                tf.write(str(chunk_total))
        except Exception as e:
            return {"success": False, "error": "Failed to record chunk info"}
    else:
        if not file_id:
            return {"success": False, "error": "Missing file_id"}
        chunk0 = "res/{}/file/.tmp_{}_{}_0".format(port_api, uid, file_id)
        if not os.path.exists(chunk0):
            return {"success": False, "error": "Invalid file_id"}
        total_path_tmp = "res/{}/file/.tmp_{}_{}_total".format(port_api, uid, file_id)
        try:
            with open(total_path_tmp, "r") as tf:
                recorded_total = int(tf.read().strip())
        except Exception as e:
            return {"success": False, "error": "Failed to read chunk info"}
        if recorded_total != chunk_total:
            return {"success": False, "error": "chunk_total mismatch"}
    
    # 每个 chunk 写入独立文件，避免并发追加交错
    chunk_path = "res/{}/file/.tmp_{}_{}_{}".format(port_api, uid, file_id, chunk_index)
    try:
        with open(chunk_path, "wb") as f:
            f.write(decoded_chunk)
    except Exception as e:
        return {"success": False, "error": "Write failed"}
    
    # 最后一块：校验完整性、合并、计算哈希并存储
    if chunk_index == chunk_total - 1:
        combined = "res/{}/file/.tmp_{}_final_{}".format(port_api, file_id, random.getrandbits(64))
        total_path_tmp = "res/{}/file/.tmp_{}_{}_total".format(port_api, uid, file_id)
        try:
            # 校验所有 chunk 均已接收
            for i in range(chunk_total):
                cp = "res/{}/file/.tmp_{}_{}_{}".format(port_api, uid, file_id, i)
                if not os.path.exists(cp):
                    return {"success": False, "error": "Missing chunk {}".format(i)}
            
            # 合并 chunk 并增量计算哈希
            sha256_hash = hashlib.sha256()
            with open(combined, "wb") as out:
                for i in range(chunk_total):
                    cp = "res/{}/file/.tmp_{}_{}_{}".format(port_api, uid, file_id, i)
                    with open(cp, "rb") as f:
                        chunk = f.read()
                        out.write(chunk)
                        sha256_hash.update(chunk)
            
            file_hash = sha256_hash.hexdigest()
            
            if expected_hash and file_hash != expected_hash:
                os.remove(combined)
                for i in range(chunk_total):
                    try:
                        os.remove("res/{}/file/.tmp_{}_{}_{}".format(port_api, uid, file_id, i))
                    except OSError:
                        pass
                try:
                    os.remove(total_path_tmp)
                except OSError:
                    pass
                return {"success": False, "error": "Hash verification failed"}
            
            final_path = "res/{}/file/{}.file".format(port_api, file_hash)
            os.rename(combined, final_path)
            
            if file_cursor:
                file_cursor.tag_file(uid, file_name, time.time(), file_hash)
                remove_outdate(port_api, file_cursor)
            
            # 清理 chunk 临时文件
            for i in range(chunk_total):
                try:
                    os.remove("res/{}/file/.tmp_{}_{}_{}".format(port_api, uid, file_id, i))
                except OSError:
                    pass
            try:
                os.remove(total_path_tmp)
            except OSError:
                pass
            
            return {"success": True, "file_hash": file_hash, "verified": expected_hash is not None}
        except Exception as e:
            if os.path.exists(combined):
                os.remove(combined)
            for i in range(chunk_total):
                try:
                    os.remove("res/{}/file/.tmp_{}_{}_{}".format(port_api, uid, file_id, i))
                except OSError:
                    pass
            try:
                os.remove(total_path_tmp)
            except OSError:
                pass
            return {"success": False, "error": "Finalization failed"}
    
    return {"success": True, "file_id": file_id}





# 建议添加一个health check函数，定期检查文件存储目录中的文件是否存在，并删除数据库中对应的记录，以防止无效文件占用服务器空间。