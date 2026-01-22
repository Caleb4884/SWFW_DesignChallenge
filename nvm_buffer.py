
import json
import os
import time


class NvmBuffer:

    def __init__(
        self,
        log_path: str,
        state_path: str,
        compact_min_bytes: int = 256_000,      # compact when log >= this many bytes
        compact_min_interval_s: float = 2.0,   # and not more often than this
        fsync_appends: bool = False,
    ):
        self.log_path = log_path
        self.state_path = state_path

        self.last_acked_seq = -1

        # replay cursor: byte offset into the log file
        self._replay_pos = 0

        # compaction tuning
        self._compact_min_bytes = compact_min_bytes
        self._compact_min_interval_s = compact_min_interval_s
        self._last_compact_time = 0.0

        self._fsync_appends = fsync_appends

        # ensure log file exists
        if not os.path.exists(self.log_path):
            open(self.log_path, "a", encoding="utf-8").close()

    # -------------------- helpers --------------------
    # Watermark handling for removing old buffered data
    def _persist_state(self) -> None:
        tmp = self.state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(str(self.last_acked_seq))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.state_path)

    # Load from file most recent acked number
    def _load_state(self) -> None:
        if os.path.exists(self.state_path):
            try:
                with open(self.state_path, "r", encoding="utf-8") as f:
                    self.last_acked_seq = int(f.read().strip())
            except Exception:
                self.last_acked_seq = -1
        else:
            self.last_acked_seq = -1

    def load(self) -> int:

        self._load_state()

        next_seq = self.last_acked_seq + 1

        # Use None as sentinel so byte offset 0 is not ambiguous.
        first_unacked_pos = None
        pos = 0

        if os.path.exists(self.log_path):
            with open(self.log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_pos = pos
                    #  byte offsets: compute using encoded line length
                    pos += len(line.encode("utf-8"))

                    line_str = line.strip()
                    if not line_str:
                        continue

                    try:
                        msg = json.loads(line_str)
                    except json.JSONDecodeError:
                        continue

                    seq = msg.get("seq")
                    if isinstance(seq, int) and seq >= next_seq:
                        next_seq = seq + 1

                    if (
                        isinstance(seq, int)
                        and seq > self.last_acked_seq
                        and first_unacked_pos is None
                    ):
                        first_unacked_pos = line_pos

        # replay from first unacked; if none, replay from end (no work)
        if first_unacked_pos is None:
            self._replay_pos = os.path.getsize(
                self.log_path) if os.path.exists(self.log_path) else 0
        else:
            self._replay_pos = first_unacked_pos

        return next_seq

    def append(self, msg: dict) -> None:
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(msg) + "\n")
            f.flush()
            if self._fsync_appends:
                os.fsync(f.fileno())

    def reset_replay(self) -> None:
        # Recompute replay start by scanning (ok for challenge scale).
        self.load()

    def iter_unacked_batch(self, max_msgs: int):

        if max_msgs <= 0:
            return
        if not os.path.exists(self.log_path):
            return

        with open(self.log_path, "r", encoding="utf-8") as f:
            f.seek(self._replay_pos)
            count = 0

            while count < max_msgs:
                line = f.readline()
                if not line:
                    break

                line_str = line.strip()
                if not line_str:
                    self._replay_pos = f.tell()
                    continue

                try:
                    msg = json.loads(line_str)
                except json.JSONDecodeError:
                    self._replay_pos = f.tell()
                    continue

                seq = msg.get("seq")
                if isinstance(seq, int) and seq <= self.last_acked_seq:
                    # already acked; advance cursor
                    self._replay_pos = f.tell()
                    continue

                yield msg
                count += 1
                self._replay_pos = f.tell()

    def mark_acked(self, ack_seq: int) -> None:

        if not isinstance(ack_seq, int):
            return
        if ack_seq <= self.last_acked_seq:
            return

        self.last_acked_seq = ack_seq
        self._persist_state()
        self._compact_if_needed()

    # -------------------- compaction --------------------

    def _compact_if_needed(self) -> None:

        now = time.time()
        if (now - self._last_compact_time) < self._compact_min_interval_s:
            return

        if not os.path.exists(self.log_path):
            return

        try:
            size = os.path.getsize(self.log_path)
        except OSError:
            return

        if size < self._compact_min_bytes:
            return

        tmp = self.log_path + ".tmp"

        with open(self.log_path, "r", encoding="utf-8") as src, open(tmp, "w", encoding="utf-8") as dst:
            for line in src:
                line_str = line.strip()
                if not line_str:
                    continue

                try:
                    msg = json.loads(line_str)
                except json.JSONDecodeError:
                    continue

                seq = msg.get("seq")
                if isinstance(seq, int) and seq > self.last_acked_seq:
                    dst.write(json.dumps(msg) + "\n")

            dst.flush()
            os.fsync(dst.fileno())

        os.replace(tmp, self.log_path)

        # Offsets changed; replay from start of the compacted file.
        self._replay_pos = 0
        self._last_compact_time = now
