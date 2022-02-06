package com.zuehlke.securesoftwaredevelopment.repository;

import com.zuehlke.securesoftwaredevelopment.config.AuditLogger;
import com.zuehlke.securesoftwaredevelopment.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Repository
public class OrderRepository {

    private DataSource dataSource;
    private static final Logger LOG = LoggerFactory.getLogger(OrderRepository.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(OrderRepository.class);
    public OrderRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }


    public List<Food> getMenu(int id) {
        List<Food> menu = new ArrayList<>();
        String sqlQuery = "SELECT id, name FROM food WHERE restaurantId=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {
            while (rs.next()) {
                menu.add(createFood(rs));
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in OrderRepository.java - getMenu(int id)", e.getMessage());
        }

        return menu;
    }

    private Food createFood(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        return new Food(id, name);
    }

    public void insertNewOrder(NewOrder newOrder, int userId) {
        LocalDate date = LocalDate.now();
        String sqlQuery = "INSERT INTO delivery (isDone, userId, restaurantId, addressId, date, comment)" +
                "values (FALSE, ?, ?, ?, '" + date.getYear() + "-" + date.getMonthValue() + "-" + date.getDayOfMonth() + "', ? )";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);) {

            preparedStatement.setInt(1, userId);
            preparedStatement.setInt(2, newOrder.getRestaurantId());
            preparedStatement.setInt(3, newOrder.getAddress());
            preparedStatement.setString(4, newOrder.getComment());
            preparedStatement.executeUpdate();

            Statement statement = connection.createStatement();
            auditLogger.audit("Delivery insert id = " + newOrder.getRestaurantId());
            String query = "SELECT MAX(id) FROM delivery";
            ResultSet rs = statement.executeQuery(query);

            if (rs.next()) {

                int deliveryId = rs.getInt(1);
                sqlQuery = "INSERT INTO delivery_item (amount, foodId, deliveryId) values";
                for (int i = 0; i < newOrder.getItems().length; i++) {
                    FoodItem item = newOrder.getItems()[i];
                    String deliveryItem = "";
                    if (i > 0) {
                        deliveryItem = ",";
                    }
                    deliveryItem += "(" + item.getAmount() + ", " + item.getFoodId() + ", " + deliveryId + ")";
                    sqlQuery += deliveryItem;
                }
                statement.executeUpdate(sqlQuery);
                auditLogger.audit("Delivery items insert");
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in OrderRepository.java - insertNewOrder(NewOrder newOrder, int userId)", e.getMessage());
        }


    }

    public Object getAddresses(int userId) {
        List<Address> addresses = new ArrayList<>();
        String sqlQuery = "SELECT id, name FROM address WHERE userId=" + userId;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(sqlQuery)) {
            while (rs.next()) {
                addresses.add(createAddress(rs));
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in OrderRepository.java - getAddresses(int userId)", e.getMessage());
        }
        return addresses;
    }

    private Address createAddress(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        return new Address(id, name);

    }
}
