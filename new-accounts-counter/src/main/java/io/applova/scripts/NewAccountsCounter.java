package io.applova.scripts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class NewAccountsCounter {
    private static HashSet<String> businessUsers;

    public static void main(String[] args) {
        String url = "jdbc:postgresql://postgres.apptizer.io:5432/apptizer";
        String user = "postgres";
        String password = "password";

        String query = "";
        LocalDate startDate;
        LocalDate endDate;

        if (args.length == 3) {
            List<String> emails = getEmailsFrom(args[0]);
            startDate = LocalDate.parse(args[1]);
            endDate = LocalDate.parse(args[2]);

            for (String email : emails) {
                query = query.concat("select businessid, name, country, businessuser, created_date from business where email = '").concat(email).concat("' and created_date between '").concat(startDate.toString()).concat("' and '").concat(endDate.toString()).concat("';");
            }
        } else {
            startDate = LocalDate.parse(args[0]);
            endDate = LocalDate.parse(args[1]);
            query = query.concat("select businessid, name, country, businessuser, created_date from business where created_date between '").concat(startDate.toString()).concat("' and '").concat(endDate.toString()).concat("';");
        }

        businessUsers = new HashSet<>();
        BufferedWriter bufferedWriter = null;

        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.execute();

            bufferedWriter = new BufferedWriter(new FileWriter("new-accounts-counter-results.csv"));
            bufferedWriter.write("Business ID,Business Name,Country,Business User,Created Date");
            bufferedWriter.newLine();

            do {
                try (ResultSet resultSet = preparedStatement.getResultSet()) {

                    if (resultSet.next()) {
                        printRecordsFrom(resultSet, bufferedWriter);
                    }
                }
            } while (preparedStatement.getMoreResults());

            printTotal(bufferedWriter, startDate.toString(), endDate.minusDays(1).toString());

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(NewAccountsCounter.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bufferedWriter != null) {
                try {
                    bufferedWriter.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<String> getEmailsFrom(String filePath) {
        List<String> emails = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
             CSVParser parser = CSVFormat.DEFAULT.withDelimiter(',').withHeader().parse(bufferedReader)) {
            for (CSVRecord csvRecord : parser) {
                emails.add(csvRecord.get("Email"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return emails;
    }

    private static void printRecordsFrom(
            ResultSet resultSet, BufferedWriter bufferedWriter) throws SQLException, IOException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        do {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String value = "\"" + resultSet.getString(i) + "\"";
                if (resultSetMetaData.getColumnLabel(i).equals("businessuser")) {
                    businessUsers.add(value);
                }
                if (i < resultSetMetaData.getColumnCount()) {
                    value = value.concat(",");
                }
                bufferedWriter.append(value);
            }
            bufferedWriter.newLine();
        } while (resultSet.next());
    }

    private static void printTotal(
            BufferedWriter bufferedWriter, String startDate, String endDate) throws IOException {
        bufferedWriter.append("\nNumber of merchants who created a new account from ").append(startDate).append(" to ").append(endDate).append(": ").append(String.valueOf(businessUsers.size()));
    }
}