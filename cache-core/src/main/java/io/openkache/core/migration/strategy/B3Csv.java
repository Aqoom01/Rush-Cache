package io.openkache.core.migration.strategy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** B3 계획·결과의 UTF-8 CSV. 쉼표·따옴표·줄바꿈을 보존하며 외부 도구를 호출하지 않는다. */
public final class B3Csv {
    private B3Csv(){}
    public static String write(List<String> fields,List<? extends Map<String,?>> rows){var out=new StringBuilder("\ufeff");append(out,fields);for(var row:rows)append(out,fields.stream().map(k->{Object v=row.get(k);return v==null?"":v.toString();}).toList());return out.toString();}
    private static void append(StringBuilder out,List<String> row){for(int i=0;i<row.size();i++){if(i>0)out.append(',');String s=row.get(i);if(s.indexOf(',')>=0||s.indexOf('"')>=0||s.indexOf('\n')>=0||s.indexOf('\r')>=0)out.append('"').append(s.replace("\"","\"\"")).append('"');else out.append(s);}out.append('\n');}
    public static List<Map<String,String>> read(String text){if(text.startsWith("\ufeff"))text=text.substring(1);var records=new ArrayList<List<String>>();var row=new ArrayList<String>();var field=new StringBuilder();boolean quote=false;
        for(int i=0;i<text.length();i++){char c=text.charAt(i);if(c=='"'){if(quote&&i+1<text.length()&&text.charAt(i+1)=='"'){field.append('"');i++;}else quote=!quote;}else if(c==','&&!quote){row.add(field.toString());field.setLength(0);}else if((c=='\n'||c=='\r')&&!quote){if(c=='\r'&&i+1<text.length()&&text.charAt(i+1)=='\n')i++;row.add(field.toString());field.setLength(0);records.add(List.copyOf(row));row.clear();}else field.append(c);}
        if(quote)throw new IllegalArgumentException("unclosed CSV quote");if(!row.isEmpty()||!field.isEmpty()){row.add(field.toString());records.add(List.copyOf(row));}if(records.isEmpty())return List.of();var fields=records.removeFirst();var out=new ArrayList<Map<String,String>>();for(var record:records){if(record.size()!=fields.size())throw new IllegalArgumentException("CSV width mismatch");var mapped=new LinkedHashMap<String,String>();for(int i=0;i<fields.size();i++)if(mapped.put(fields.get(i),record.get(i))!=null)throw new IllegalArgumentException("duplicate CSV field");out.add(Map.copyOf(mapped));}return List.copyOf(out);}
}
