public class StringFormat {
    public static void main(String[] args) {
        String database = "kingcall";
        String table = "king";
        String basesql = String.format("insert into %s.%s(%s) values(%s)",database,table,"%s","%s" );
        System.out.println(basesql);
    }
}
