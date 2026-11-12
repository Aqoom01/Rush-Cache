package io.openkache.runner.benchmark.measurement;

import io.openkache.core.replication.ionia.IoniaCommands;
import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.runner.benchmark.correctness.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.*;
import java.util.*;

/**
 * correctness 전용 새 디렉터리에 전수 payload 이력/ACK/검사 결과를 저장한다.
 * 기존 성능 result의 not_evaluated를 몰래 바꾸지 않는다. run/clock/token·hash로 별도 증거를 연결한다.
 * 로컬 raw payload는 base64로 보존하며 Notion/외부 서비스에 업로드하지 않는다.
 */
public final class CorrectnessRunWriter {
    private CorrectnessRunWriter() {}
    public record Export(Path directory,HistoryChecker.Result historyResult,String integritySha256) {}
    public static Export write(Path directory,HistoryRecorder.Snapshot history,Map<Bytes,Bytes> initialState,
            HistoryChecker.Limits limits,AckLedger.Ledger ledger,AckLedger.Audit audit)throws IOException{
        if(ledger!=null&&!ledger.historyToken().equals(history.token())||audit!=null&&!audit.historyToken().equals(history.token())){throw new IllegalArgumentException("correctness evidence token mismatch");}
        if(audit!=null){
            if(ledger==null){throw new IllegalArgumentException("audit requires its ledger");}
            var reads=audit.items().stream().map(AckLedger.Item::read).filter(Objects::nonNull).toList();
            if(!AckLedger.audit(ledger,audit.barrierLabel(),audit.barrierNanos(),audit.readiness(),reads).equals(audit)){throw new IllegalArgumentException("ACK audit does not match ledger/read evidence");}
        }
        var checked=HistoryChecker.check(history,initialState,limits);
        Path target=directory.toAbsolutePath().normalize();Files.createDirectory(target);
        write(target.resolve("STARTED"),"Correctness artifact; COMPLETE is required.\n");
        List<Map<String,Object>> files=new ArrayList<>();
        StringBuilder rows=new StringBuilder();
        for(var op:history.operations()){
            List<Map<String,Object>> calls=new ArrayList<>();
            for(var call:op.calls()){calls.add(map("submission",call.submission(),"invoked_ns",call.invokedNanos(),"observed_ns",call.observedNanos(),
                    "kind",call.kind(),"error_kind",call.errorKind(),"outcome",outcome(call.outcome())));}
            rows.append(BenchmarkJson.encode(map("run_id",history.runId(),"load_slot",history.loadSlot(),"clock_id",history.clockId(),
                    "request",request(op.request()),"calls",calls))).append('\n');
        }
        add(target,"history.jsonl",rows.toString(),files);
        Map<String,Object> ledgerData=ledger==null?map("status","not_run","reason","no_reserved_ack_ledger"):
                map("history_token",ledger.historyToken(),"exclusive_namespace",ledger.exclusiveNamespace(),
                        "entries",ledger.entries().stream().map(CorrectnessRunWriter::entry).toList(),
                        "exclusions",ledger.exclusions().entrySet().stream().map(e->map("key",bytes(e.getKey()),"reason",e.getValue())).toList());
        add(target,"ack-ledger.json",BenchmarkJson.encode(ledgerData)+"\n",files);
        Object auditData=audit==null?map("status","not_run","reason","no_post_barrier_audit"):
                map("history_token",audit.historyToken(),"verdict",audit.verdict(),"reason",audit.reason(),"barrier_label",audit.barrierLabel(),
                        "barrier_ns",audit.barrierNanos(),"clock_id",history.clockId(),"readiness",audit.readiness(),"excluded_keys",audit.excludedKeys(),
                        "items",audit.items().stream().map(i->map("entry",entry(i.entry()),"status",i.status(),"reason",i.reason(),
                                "read",i.read()==null?null:map("request",request(i.read().request()),"clock_id",i.read().clockId(),"outcome",outcome(i.read().outcome())))).toList());
        add(target,"ack-audit.json",BenchmarkJson.encode(auditData)+"\n",files);
        List<Map<String,Object>> initial=initialState==null?null:initialState.entrySet().stream()
                .sorted(Comparator.comparing(e->Base64.getEncoder().encodeToString(e.getKey().toByteArray())))
                .map(e->map("key",bytes(e.getKey()),"value",bytes(e.getValue()))).toList();
        var report=map("schema_version","benchmark-correctness-v1","history_token",history.token(),"run_id",history.runId(),"load_slot",history.loadSlot(),
                "clock_id",history.clockId(),"clock_scope","single_adapter_monotonic_domain","sealed_ns",history.sealedNanos(),
                "collection_complete",history.collectionComplete(),"collection_failures",history.failures(),"operations",history.operations().size(),
                "submissions",history.submissions(),"pending_calls",history.pendingCalls(),"retained_payload_bytes",history.retainedPayloadBytes(),
                "record_limits",history.limits(),"checker_limits",limits,"initial_state_attested",initialState!=null,"initial_state",initial,
                "history_result",checked,"ack_audit_verdict",audit==null?"not_run":audit.verdict(),
                "limitations",List.of("bounded_offline_check_not_a_proof_of_all_executions","uncertain_operations_never_pass",
                        "single_key_ops_no_transactions_or_ttl","audit_namespace_exclusivity_is_attested_not_discovered",
                        "not_a_power_loss_or_multi_host_durability_proof","full_payload_recording_not_performance_sampling"));
        add(target,"correctness.json",BenchmarkJson.encode(report)+"\n",files);
        var integrity=map("schema_version","benchmark-correctness-integrity-v1","complete",true,"history_token",history.token(),
                "files",List.copyOf(files),"collection_complete",history.collectionComplete(),"history_verdict",checked.verdict(),
                "ack_verdict",audit==null?"not_run":audit.verdict());
        write(target.resolve("integrity.json"),BenchmarkJson.encode(integrity)+"\n");
        String digest=sha(Files.readAllBytes(target.resolve("integrity.json")));write(target.resolve("COMPLETE"),"integrity_sha256="+digest+"\n");
        return new Export(target,checked,digest);
    }
    private static Map<String,Object> entry(AckLedger.Entry e){return map("id",e.id(),"key",bytes(e.key()),"expected_value",bytes(e.expectedValue()),"acknowledged_ns",e.acknowledgedNanos(),"deletion",e.deletion());}
    private static Map<String,Object> request(LogicalRequest r){return map("phase",r.phase(),"global_index",r.globalIndex(),"stream_id",r.streamId(),"client_id",r.clientId(),"sequence",r.sequence(),
            "operation",ClientOutcome.Operation.of(r.command()),"key",bytes(IoniaCommands.key(r.command())),"input_value",r.command() instanceof Command.Set s?bytes(s.value()):null);}
    private static Map<String,Object> outcome(ClientOutcome o){return o==null?null:map("id",o.id(),"operation",o.operation(),"category",o.category(),"reason",o.reason(),
            "invoked_ns",o.invokedNanos(),"decided_ns",o.decidedNanos(),"terminal_ns",o.terminalNanos(),"wire_client_id",o.wireClientId(),"wire_operation_id",o.wireOperationId(),
            "rounds",o.rounds(),"attempts",o.attempts(),"result",o.result()==null?null:map("status",o.result().status(),"value",o.result().value().map(CorrectnessRunWriter::bytes).orElse(null),
                    "value_version",o.result().valueVersion(),"routing_version",o.result().routingVersion().value(),"ownership_epoch",o.result().ownershipEpoch().value()));}
    private static Map<String,Object> bytes(Bytes value){if(value==null){return null;}byte[] raw=value.toByteArray();return map("base64",Base64.getEncoder().encodeToString(raw),"length",raw.length,"sha256",sha(raw));}
    private static Map<String,Object> map(Object... pairs){return BenchmarkRunWriter.map(pairs);}
    private static String sha(byte[] value){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value));}catch(NoSuchAlgorithmException impossible){throw new IllegalStateException(impossible);}}
    private static void add(Path root,String name,String value,List<Map<String,Object>> files)throws IOException{Path path=root.resolve(name);write(path,value);files.add(map("path",name,"sha256",sha(Files.readAllBytes(path))));}
    private static void write(Path file,String value)throws IOException{
        Path part=file.resolveSibling(file.getFileName()+".part");Files.writeString(part,value,StandardCharsets.UTF_8,StandardOpenOption.CREATE_NEW);
        Files.move(part,file,StandardCopyOption.ATOMIC_MOVE);
    }
}
