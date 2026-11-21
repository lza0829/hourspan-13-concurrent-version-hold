"""并发版本持锁 · concurrent_version_hold

已实现。全局架构：**甲·冲突入搁置**（默认，可用 settings.conflict_mode 切到
乙·冲突整票回滚做对照核证；同一时刻只贯彻一种）。

实现面见 ``service``：
  * 租约 ``version_leases``（按 store_id 单持租，带 TTL/过期回收）
  * 未提交草稿 ``version_drafts``（跑次 open 即挡后到）
  * 搁置行 ``version_holds``（甲冲突证据：键+冲突 run 对+当时 is_current 快照）
  * 回执 ``apply_receipts``（乙冲突证据：整票回滚回执）
  * 只读核证 ``build_proof`` / ``GET /api/concurrency/proof``

正式维写入在 ``app.services.scd`` 中持租完成；检测入口在
``app.services.detect`` 落 open 跑次 + 逐键草稿。
"""

from app.modules.concurrent_version_hold import service

__all__ = ["service"]


def not_implemented():  # 兼容旧空桩调用点
    raise NotImplementedError("concurrent_version_hold 已实现，请用 service API")
