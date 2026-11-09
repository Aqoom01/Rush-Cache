"""ACK 부족 시 false OK와 잘못된 저장/복구 집계를 거절하는지 확인."""
import unittest
from pb_fault_bench import validate

def fixture(kind):
    blocked=kind=="ACK_BLOCK"
    return {
        "schema":"a1-real-pb-backup-fault-1","ok":True,"scenario":kind,
        "firstStatus":7 if blocked else 0,
        "copiesAfterFirst":[{"node":i,"status":1 if blocked and i==0 else 0,"matches":not(blocked and i==0)} for i in range(3)],
        "retryStatus":0 if blocked else None,"followupStatus":0,"protocolProbeStatuses":[0,0],
        "finalCopies":6,"missingCopies":0,"mismatchedCopies":0,
        "statesAfter":[{"epoch":1} for _ in range(3)],"firstSentNs":0,
        "firstResponseNs":200_000_000,"deadlineMs":200 if blocked else 1000,
        "holdMs":800 if blocked else 200 if kind=="SLOW_BACKUP" else 0}

class BackupFaultTest(unittest.TestCase):
    def test_each_scenario_is_valid(self):
        for kind in ("BASELINE","SLOW_BACKUP","ACK_BLOCK"):
            validate(fixture(kind))
    def test_false_success_under_ack_block_is_rejected(self):
        r=fixture("ACK_BLOCK")
        r["firstStatus"]=0
        with self.assertRaises(AssertionError):
            validate(r)
        r=fixture("ACK_BLOCK")
        r["copiesAfterFirst"][1]["matches"]=False
        with self.assertRaises(AssertionError):
            validate(r)
    def test_no_overlap_or_stray_response_or_unexpected_promotion_is_rejected(self):
        r=fixture("SLOW_BACKUP")
        r["firstResponseNs"]=1_000_000
        with self.assertRaises(AssertionError):
            validate(r)
        for field,value in (("protocolProbeStatuses",[0,7]),("finalCopies",5)):
            r=fixture("BASELINE")
            r[field]=value
            with self.assertRaises(AssertionError):
                validate(r)
        r=fixture("BASELINE")
        r["statesAfter"][1]["epoch"]=2
        with self.assertRaises(AssertionError):
            validate(r)

if __name__=="__main__":
    unittest.main()
