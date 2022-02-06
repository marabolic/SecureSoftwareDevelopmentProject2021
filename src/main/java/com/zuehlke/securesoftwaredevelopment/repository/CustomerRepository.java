package com.zuehlke.securesoftwaredevelopment.repository;

import com.zuehlke.securesoftwaredevelopment.config.AuditLogger;
import com.zuehlke.securesoftwaredevelopment.config.Entity;
import com.zuehlke.securesoftwaredevelopment.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Repository
public class CustomerRepository {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerRepository.class);
    private static final AuditLogger auditLogger = AuditLogger.getAuditLogger(CustomerRepository.class);

    private DataSource dataSource;

    public CustomerRepository(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    private Person createPersonFromResultSet(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String firstName = rs.getString(2);
        String lastName = rs.getString(3);
        String personalNumber = rs.getString(4);
        String address = rs.getString(5);
        return new Person(id, firstName, lastName, personalNumber, address);
    }

    public List<Customer> getCustomers() {
        List<com.zuehlke.securesoftwaredevelopment.domain.Customer> customers = new ArrayList<com.zuehlke.securesoftwaredevelopment.domain.Customer>();
        String query = "SELECT id, username FROM users";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                customers.add(createCustomer(rs));
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - getCustomers()", e.getMessage());
        }
        return customers;
    }

    private com.zuehlke.securesoftwaredevelopment.domain.Customer createCustomer(ResultSet rs) throws SQLException {
        return new com.zuehlke.securesoftwaredevelopment.domain.Customer(rs.getInt(1), rs.getString(2));
    }

    public List<Restaurant> getRestaurants() {
        List<Restaurant> restaurants = new ArrayList<Restaurant>();
        String query = "SELECT r.id, r.name, r.address, rt.name  FROM restaurant AS r JOIN restaurant_type AS rt ON r.typeId = rt.id ";
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(query)) {

            while (rs.next()) {
                restaurants.add(createRestaurant(rs));
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - getRestaurants()", e.getMessage());
        }
        return restaurants;
    }

    private Restaurant createRestaurant(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        String address = rs.getString(3);
        String type = rs.getString(4);

        return new Restaurant(id, name, address, type);
    }


    public Object getRestaurant(String id) {
        String query = "SELECT r.id, r.name, r.address, rt.name  FROM restaurant AS r JOIN restaurant_type AS rt ON r.typeId = rt.id WHERE r.id=?" ;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query);) {

            statement.setString(1, id);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                return createRestaurant(rs);
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java -  getRestaurant(String id)", e.getMessage());
        }
        return null;
    }

    public void deleteRestaurant(int id) {
        String query = "DELETE FROM restaurant WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Restaurant delete id = " + id);
        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - deleteRestaurant(int id)", e.getMessage());
        }
    }

    public void updateRestaurant(RestaurantUpdate restaurantUpdate) {
        Restaurant restaurant = (Restaurant) getRestaurant(String.valueOf(restaurantUpdate.getId()));
        String query = "UPDATE restaurant SET name = ?, address= ?, typeId = ? WHERE id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, restaurantUpdate.getName());
            statement.setString(2, restaurantUpdate.getAddress());
            statement.setInt(3, restaurantUpdate.getRestaurantType());
            statement.setInt(4, restaurantUpdate.getId());
            statement.executeUpdate();

            String previous = "Name: " + restaurant.getName() + "Address: " + restaurant.getAddress() + "Restaurant type: "
                    + restaurant.getRestaurantType();

            String current = "Name: " + restaurantUpdate.getName() + "Address: " + restaurantUpdate.getAddress() + "Restaurant type: "
                    + restaurantUpdate.getRestaurantType();

            auditLogger.auditChange(new Entity(
                    "restaurant.update",
                    String.valueOf(restaurant.getId()),
                    previous,
                    current
            ));
        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - updateRestaurant(RestaurantUpdate restaurantUpdate)", e.getMessage());
        }
    }

    public Customer getCustomer(String id) {
        String sqlQuery = "SELECT id, username, password FROM users WHERE id=?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlQuery);) {

            statement.setString(1, id);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                return createCustomerWithPassword(rs);
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - getCustomer(String id)", e.getMessage());
        }
        return null;
    }

    private Customer createCustomerWithPassword(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String username = rs.getString(2);
        String password = rs.getString(3);
        return new Customer(id, username, password);
    }


    public void deleteCustomer(String id) {
        String query = "DELETE FROM users WHERE id=?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, id);
            statement.executeUpdate();
            auditLogger.audit("Customer delete id = " + id);
        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - deleteCustomer(String id)", e.getMessage());
        }
    }

    public void updateCustomer(CustomerUpdate customerUpdate) {
        Customer customer = (Customer)getCustomer(String.valueOf(customerUpdate.getId()));
        String query = "UPDATE users SET username = ?, password=? WHERE id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, customerUpdate.getUsername());
            statement.setString(2, customerUpdate.getPassword());
            statement.setInt(3, customerUpdate.getId());
            statement.executeUpdate();

            String previous = "Username: " + customer.getUsername() + "Password: " + customer.getPassword();
            String current = "Username: " + customerUpdate.getUsername() + "Password: " + customerUpdate.getPassword();

            auditLogger.auditChange(new Entity(
                    "customer.update",
                    String.valueOf(customerUpdate.getId()),
                    previous,
                    current
            ));
        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - updateCustomer(CustomerUpdate customerUpdate)", e.getMessage());
        }
    }

    public List<Address> getAddresses(String id) {
        String sqlQuery = "SELECT id, name FROM address WHERE userId=?";
        List<Address> addresses = new ArrayList<Address>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sqlQuery);) {

            statement.setString(1, id);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                addresses.add(createAddress(rs));
            }

        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - getAddresses(String id)", e.getMessage());
        }
        return addresses;
    }

    private Address createAddress(ResultSet rs) throws SQLException {
        int id = rs.getInt(1);
        String name = rs.getString(2);
        return new Address(id, name);
    }

    public void deleteCustomerAddress(int id) {
        String query = "DELETE FROM address WHERE id=" + id;
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.executeUpdate(query);
            auditLogger.audit("Customer address delete id = " + id);
        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - deleteCustomerAddress(int id)", e.getMessage());
        }
    }

    public void updateCustomerAddress(Address address) {
        String query = "UPDATE address SET name = ? WHERE id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, address.getName());
            statement.setInt(2, address.getId());
            statement.executeUpdate();
            auditLogger.audit("Customer address update id = " + address.getId());
        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - updateCustomerAddress(Address address)", e.getMessage());
        }
    }

    public void putCustomerAddress(NewAddress newAddress) {
        String query = "INSERT INTO address (name, userId) VALUES (? , ?)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(query)) {

            statement.setString(1, newAddress.getName());
            statement.setInt(2, newAddress.getUserId());
            statement.executeUpdate();
            auditLogger.audit("Customer address insert id = " + newAddress.getUserId());
        } catch (SQLException e) {
            LOG.warn("SQLException in CustomerRepository.java - putCustomerAddress(NewAddress newAddress)", e.getMessage());
        }
    }
}
