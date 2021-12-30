package io.applova.scripts;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UpdatedProductsListGenerator {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://postgres.apptizer.io:5432/apptizer";
        String user = "postgres";
        String password = "password";

        String query = "";
        LocalDate startDate;
        LocalDate endDate;

        if (args.length == 3) {
            List<String> businessIds = getBusinessIdsFrom(args[0]);
            startDate = LocalDate.parse(args[1]);
            endDate = LocalDate.parse(args[2]);

            for (String businessId : businessIds) {
                query = query.concat("select businessid, productid, product.name, coalesce(product.last_updated_date, product.created_date) as last_updated_date from product, business where product.business = business.businessid and businessid = '").concat(businessId).concat("' and (product.created_date between '").concat(startDate.toString()).concat("' and '").concat(endDate.toString()).concat("' or product.last_updated_date between '").concat(startDate.toString()).concat("' and '").concat(endDate.toString()).concat("');");
            }
        } else {
            startDate = LocalDate.parse(args[0]);
            endDate = LocalDate.parse(args[1]);
            query = query.concat("select businessid, productid, product.name, coalesce(product.last_updated_date, product.created_date) as last_updated_date from product, business where product.business = business.businessid and (product.created_date between '").concat(startDate.toString()).concat("' and '").concat(endDate.toString()).concat("' or product.last_updated_date between '").concat(startDate.toString()).concat("' and '").concat(endDate.toString()).concat("')");
        }

        BufferedWriter bufferedWriter = null;

        try (Connection connection = DriverManager.getConnection(url, user, password);
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {

            preparedStatement.execute();

            bufferedWriter = new BufferedWriter(new FileWriter("created-or-updated-products-list.csv"));
            bufferedWriter.write("Business ID,Product ID,Product Name,Created / Last Updated Date");
            bufferedWriter.newLine();

            do {
                try (ResultSet resultSet = preparedStatement.getResultSet()) {

                    if (resultSet.next()) {
                        printRecordsFrom(resultSet, bufferedWriter);
                    }
                }
            } while (preparedStatement.getMoreResults());

        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(UpdatedProductsListGenerator.class.getName());
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

    private static List<String> getBusinessIdsFrom(String filePath) {
        List<String> businessIds = new ArrayList<>();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
             CSVParser parser = CSVFormat.DEFAULT.withDelimiter(',').withHeader().parse(bufferedReader)) {
            for (CSVRecord csvRecord : parser) {
                businessIds.add(csvRecord.get("Business ID"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return businessIds;
    }

    private static void printRecordsFrom(
            ResultSet resultSet, BufferedWriter bufferedWriter) throws SQLException, IOException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        do {
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                String value = "\"" + resultSet.getString(i) + "\"";
                if (i < resultSetMetaData.getColumnCount()) {
                    value = value.concat(",");
                }
                bufferedWriter.append(value);
            }
            bufferedWriter.newLine();
        } while (resultSet.next());
    }
}
