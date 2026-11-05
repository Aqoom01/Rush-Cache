package io.openkache.core.migration.experiment;

import static org.junit.jupiter.api.Assertions.*;
import io.openkache.core.migration.strategy.B3Csv;
import io.openkache.core.migration.strategy.B3Json;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JavaPolicyBackendTest {
    static Map<String,Object> fixture(String name) throws IOException {try(var stream=JavaPolicyBackendTest.class.getResourceAsStream(name)){assertNotNull(stream);return B3Json.object(B3Json.decode(new String(stream.readAllBytes(),StandardCharsets.UTF_8)));}}
    @Test void matrixPreservesOriginalResearchCoverageWithoutExecutingIt(){assertEquals(720,ExecutionPolicyResearchMain.specs("full").size());assertEquals(96,ExecutionPolicyResearchMain.specs("quick").size());assertEquals(280,LiteratureResearchMain.expected("full"));assertEquals(112,LiteratureResearchMain.expected("quick"));}
    @Test void countOnlyRebuildRejectsHiddenRefinementAndMethodSpecificAssignment(){
        var config=io.openkache.core.migration.strategy.ScalingConfig.defaults().profile(true);
        var model=io.openkache.core.migration.strategy.PerformanceModel.calibrate(config);
        var record=B3Json.object(B3Json.decode(B3Json.encode(LiteratureResearchMain.record("estore","step",1,config,model))));
        assertDoesNotThrow(()->JavaStudyResults.validateFixedQueue(record,config));
        var metrics=B3Json.object(record.get("metrics"));metrics.put("split_units",1);record.put("metrics",metrics);
        assertThrows(IllegalArgumentException.class,()->JavaStudyResults.validateFixedQueue(record,config));metrics.put("split_units",0);
        var actions=JavaStudyResults.objects(record.get("actions"));assertFalse(actions.isEmpty());
        var assignment=JavaStudyResults.objects(actions.getFirst().get("assignments"));
        assignment.getFirst().put("holders",List.of(7));actions.getFirst().put("assignments",assignment);record.put("actions",actions);
        assertThrows(IllegalArgumentException.class,()->JavaStudyResults.validateFixedQueue(record,config));
    }
    @Test void removedSelectorsCannotEnterSchedulerMatrix(){
        assertEquals(810,SchedulerResearchMain.expected("full"));assertEquals(324,SchedulerResearchMain.expected("quick"));
        assertThrows(IllegalArgumentException.class,()->SchedulerComparisonExperiment.condition("candidate-plan","quick"));
        var condition=SchedulerComparisonExperiment.condition("conflict","quick");var expected=SchedulerComparisonExperiment.planFor(condition,"fifo");
        for(String method:SchedulerAlgorithms.METHODS)assertEquals(expected,SchedulerComparisonExperiment.planFor(condition,method));
        assertThrows(IllegalArgumentException.class,()->SchedulerComparisonExperiment.planFor(condition,"camig-selector"));
    }
    @Test void originalPythonPlacementAndFloatingDelayEvidenceReadWithoutPython() throws IOException {assertDoesNotThrow(()->LegacyPythonResults.validatePolicy(fixture("python-policy-placement.json")));assertDoesNotThrow(()->LegacyPythonResults.validatePolicy(fixture("python-policy-delay.json")));}
    @Test void resignedUnsafeLegacyAssignmentIsStillRejected() throws IOException {var row=fixture("python-policy-placement.json");row.put("assignment",List.of(9,9,9,9));var evidence=new LinkedHashMap<>(row);for(String key:List.of("evidenceHash","planningNanos","provenance"))evidence.remove(key);row.put("evidenceHash",LegacyPythonResults.digest(evidence));assertThrows(IllegalArgumentException.class,()->LegacyPythonResults.validatePolicy(row));}
    @Test void pythonNumberFormattingAndCsvEscapesArePreserved(){assertEquals("{\"a\":1e-08,\"b\":10000000.0,\"c\":1e+20}",LegacyPythonResults.encode(Map.of("a",1e-8,"b",1e7,"c",1e20)));var input=List.of(Map.of("method","a,b","reason","line\n\"quoted\""));String csv=B3Csv.write(List.of("method","reason"),input);assertEquals(input,B3Csv.read(csv));}
    @Test void logisticFitsTheSameArchivedCalibrationAsSklearn() throws IOException {
        var fixture=fixture("python-calibration.json");var expected=B3Json.object(fixture.get("record"));
        assertInstanceOf(List.class,fixture.get("queue"));var rows=((List<?>)fixture.get("queue")).stream().map(row->((List<?>)row).stream().mapToDouble(v->((Number)v).doubleValue()).toArray()).toList();
        var model=new io.openkache.core.migration.strategy.PerformanceModel(io.openkache.core.migration.strategy.ScalingConfig.defaults(),rows,List.of());double[] actual=model.coefficients();var coefficients=(List<?>)expected.get("logistic_coefficients");
        double[] original={((Number)coefficients.get(0)).doubleValue(),((Number)coefficients.get(1)).doubleValue(),((Number)expected.get("logistic_intercept")).doubleValue()};
        // Newton과 L-BFGS의 종료 허용치가 달라 계수의 임의 절대 오차 대신 같은 목적식과 판단을 검사한다.
        assertTrue(logisticLoss(rows,actual)<=logisticLoss(rows,original)+1e-8,"same L2 objective is no worse");
        for(double[] row:rows)assertEquals(original[0]*row[0]/100+original[1]*row[1]/100+original[2]<=0,model.fits(row[0],row[1]),"same archived-point decision");
        assertEquals(((Number)expected.get("capacity")).doubleValue(),model.capacity(),.1);
    }
    private static double logisticLoss(List<double[]> rows,double[] b){double result=(b[0]*b[0]+b[1]*b[1])/2000;for(var row:rows){double z=b[0]*row[0]/100+b[1]*row[1]/100+b[2],label=row[2]>.12?1:0;result+=Math.max(z,0)-z*label+Math.log1p(Math.exp(-Math.abs(z)));}return result;}
    @Test void plannerWallTimeIsPreservedButExcludedFromScientificEvidence(@TempDir Path folder) throws IOException {
        Path results=Files.createDirectory(folder.resolve("results"));var record=new LinkedHashMap<String,Object>();record.put("group","unit");record.put("scenario","fixture");record.put("method","elastman");record.put("seed",1);record.put("outcome","observed");record.put("metrics",Map.of("deadline_miss_rate",0.,"node_windows",1,"pending_actions",0));record.put("planningNanos",10L);
        Path file=results.resolve("case-0000.json");JavaStudyResults.write(file,"unit",0,record,Map.of());var first=B3Json.object(B3Json.decode(Files.readString(file)));record.put("planningNanos",20L);JavaStudyResults.write(file,"unit",0,record,Map.of());var second=B3Json.object(B3Json.decode(Files.readString(file)));assertEquals(first.get("evidenceHash"),second.get("evidenceHash"));
        var read=JavaStudyResults.read(folder,"unit",1);assertEquals(20,JavaStudyResults.n(read.getFirst(),"planningNanos"));JavaStudyResults.summarize(folder,read,"unit");assertTrue(Files.readString(folder.resolve("timings.csv")).contains("20"));assertFalse(B3Json.object(second.get("record")).containsKey("planningNanos"));
    }
}
