package com.example.demo;

import org.drools.core.impl.InternalKnowledgeBase;
import org.drools.template.jdbc.ResultSetGenerator;
import org.kie.api.io.ResourceType;
import org.kie.internal.KnowledgeBase;
import org.kie.internal.KnowledgeBaseFactory;
import org.kie.internal.builder.KnowledgeBuilder;
import org.kie.internal.builder.KnowledgeBuilderFactory;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.runtime.StatefulKnowledgeSession;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.*;

public class RuleTest {

    public static void main(String[] args) {

    }

    public void testRuleSet() throws ClassNotFoundException, SQLException, FileNotFoundException {
        Class.forName("org.postgresql.Driver");

        Connection con = DriverManager.getConnection("jdbc:postgresql://localhost:5432/MiniProject","root","anjali");
        Statement s = con.createStatement();
        String sql = "Select id, MinAge, MaxAge,Status from Age";
        ResultSet rs = s.executeQuery(sql);
        final ResultSetGenerator convertor = new ResultSetGenerator();
        final String drl = convertor.compile(rs, getRulesStream());
        System.out.println(drl);
        s.close();

        KnowledgeBuilder builder = KnowledgeBuilderFactory.newKnowledgeBuilder();
        builder.add(ResourceFactory.newByteArrayResource(drl.getBytes()), ResourceType.DRL);
        KnowledgeBase kBase = KnowledgeBaseFactory.newKnowledgeBase();
        kBase.addKnowledgePackages(builder.getKnowledgePackages());
        StatefulKnowledgeSession kSession = kBase.newStatefulKnowledgeSession();

        User user1 = new User("Rajeev",18);
        User user2 = new User("Aman",3);
        User user3 = new User("Ramesh",61);

        kSession.insert(user1);
        kSession.insert(user2);
        kSession.insert(user3);

        kSession.fireAllRules();
        System.out.println(user1.getName()+" "+user1.getStatus());
        System.out.println(user1.getName()+" "+user2.getStatus());
        System.out.println(user1.getName()+" "+user3.getStatus());

        kSession.destroy();
        kSession.dispose();


        kSession = kBase.newStatefulKnowledgeSession();
        User aa = new User("AA", 82);
        User bb = new User("BB", 2);
        User cc = new User("CC", 41);
        kSession.insert(aa);
        kSession.insert(bb);
        kSession.insert(cc);
        kSession.fireAllRules();
        System.out.println(aa.getName() + "," + aa.getStatus());
        System.out.println(bb.getName() + "," + bb.getStatus());
        System.out.println(cc.getName() + "," + cc.getStatus());
    }

    private static InputStream getRulesStream() throws FileNotFoundException {
        return new FileInputStream("C:\\Users\\Anjali_Singh\\Desktop\\rule.drt");
    }

    private static void dbOperation(String expression, Connection conn)
            throws SQLException {
        Statement st;
        st = conn.createStatement();
        int i = st.executeUpdate(expression);
        if (i == -1) {
            System.out.println("db error : " + expression);
        }
        st.close();
    }
}
