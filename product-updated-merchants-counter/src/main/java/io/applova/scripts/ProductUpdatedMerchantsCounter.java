package io.applova.scripts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ProductUpdatedMerchantsCounter {
    private static HashSet<String> businessUsers;

    public static void main(String[] args) {
        String url = "jdbc:postgresql://postgres.apptizer.io:5432/apptizer";
        String user = "postgres";
        String password = "password";

        String query = "";
        String startDate;
        String endDate;

        if (args.length == 3) {
            List<String> emails = getEmailsFrom(args[0]);
            startDate = args[1];
            endDate = args[2];

            for (String email : emails) {
                query = query.concat("select businessid, business.name, country, businessuser from business, product where businessid = product.business and email = '").concat(email).concat("' and product.created_date between '").concat(startDate).concat("' and '").concat(endDate).concat("' group by businessid, business.name, country, businessuser;");
            }
        } else {
            startDate = args[0];
            endDate = args[1];
            query = query.concat("select businessid, business.name, country, businessuser from business, product where businessid = product.business and product.created_date between '").concat(startDate).concat("' and '").concat(endDate).concat("' group by businessid, business.name, country, businessuser;");
        }

        businessUsers = new HashSet<>();
        BufferedWriter bufferedWriter = null;

        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.execute();

            bufferedWriter = new BufferedWriter(new FileWriter("product-created-or-updated-merchants-counter-results.csv"));
            bufferedWriter.write("Business ID,Business Name,Country,Business User");
            bufferedWriter.newLine();

            do {
                try (ResultSet resultSet = preparedStatement.getResultSet()) {

                    if (resultSet.next()) {
                        printRecordsFrom(resultSet, bufferedWriter);
                    }
                }
            } while (preparedStatement.getMoreResults());

            printTotal(bufferedWriter, startDate, endDate);

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(ProductUpdatedMerchantsCounter.class.getName());
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
        bufferedWriter.append("\nNumber of merchants who created or updated products from ").append(startDate).append(" to ").append(endDate).append(": ").append(String.valueOf(businessUsers.size()));
    }
}