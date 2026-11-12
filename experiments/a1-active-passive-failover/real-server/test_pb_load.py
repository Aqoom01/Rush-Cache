"""진행 중 crash 집계가 늦은 old Primary OK나 미보존 값을 성공 복구로 오인하지 않는지 검사."""
import copy
import unittest
from pb_load import classify_attempt,validate,value_for

def fixture():
    attempts=[
        {"logical":1,"node":0,"startNs":1,"sentNs":2,"endNs":3,"status":0,"responseEpoch":1},
        {"logical":2,"node":0,"startNs":20,"sentNs":21,"endNs":24,"status":"CLIENT_FAILURE"},
        {"logical":2,"node":1,"startNs":30,"sentNs":31,"endNs":40,"status":0,"responseEpoch":2}]
    return {"schema":"a1-real-pb-load-1","ok":True,"attempts":attempts,"targetCount":1,"faultCallBeginNs":22,
            "faultCallReturnNs":23,"inflightLogical":2,"preFaultOk":1,"newPrimaryOk":1,"oldPrimaryLateOk":0,
            "successes":2,"preservedSuccesses":2,"verifiedCopies":4,"missingCopies":0,"mismatchedCopies":0,
            "failoverNs":18,"inflightFirstOutcome":"CLIENT_FAILURE","inflightFinalStatus":0,
            "unresolvedLogical":0,"staleEpochStatus":5,"backupWriteStatus":4}

class LoadTraceTest(unittest.TestCase):
    def test_valid_trace_and_inflight_interval(self):
        validate(fixture())
        for field,value in (("faultCallBeginNs",19),("faultCallReturnNs",25),("preservedSuccesses",1),("missingCopies",1)):
            result=fixture()
            result[field]=value
            with self.assertRaises(AssertionError):
                validate(result)

    def test_old_primary_late_ok_is_not_new_primary_recovery(self):
        attempt={"status":0,"endNs":30,"node":0,"responseEpoch":1}
        self.assertEqual("OLD_PRIMARY_LATE_OK",classify_attempt(attempt,22))
        attempt["node"]=1
        attempt["responseEpoch"]=2
        self.assertEqual("NEW_PRIMARY_OK",classify_attempt(attempt,22))
        result=fixture()
        result["attempts"][-1]["node"]=0
        result["attempts"][-1]["responseEpoch"]=1
        with self.assertRaises(AssertionError):
            validate(result)

    def test_duplicate_success_or_wrong_payload_is_detectable(self):
        result=fixture()
        result["attempts"].insert(2,copy.deepcopy(result["attempts"][-1]))
        with self.assertRaises(AssertionError):
            validate(result)
        self.assertNotEqual(value_for(1,64),value_for(2,64))
        self.assertEqual(8192,len(value_for(2,8192)))

if __name__=="__main__":
    unittest.main()
