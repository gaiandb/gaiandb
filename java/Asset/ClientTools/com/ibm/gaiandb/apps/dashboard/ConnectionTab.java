/* Licensed Materials - Property of IBM
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.sql.Connection;
import java.util.Properties;

import javax.swing.BoxLayout;
import javax.swing.Icon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

import com.ibm.gaiandb.GaianDBConfig;
import com.ibm.gaiandb.GaianNode;
import com.ibm.gaiandb.apps.DBConnector;

//import com.ibm.gaiandb.security.client.JavaAuth;
//import com.ibm.gaiandb.security.common.KerberosToken;
//import com.ibm.security.auth.callback.Krb5CallbackHandler;
//
//import javax.security.auth.kerberos.KerberosTicket;

public class ConnectionTab extends Tab {
	
	//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";

	private static final long serialVersionUID = 6749084448973692098L;

	final JPanel pane = new JPanel();
	final JPanel connectionPanel = new JPanel(new GridBagLayout());
	
	private static final int STRING_FIELDS_WIDTH = 25;

	final JTextField hostField = new JTextField(STRING_FIELDS_WIDTH);
	final JTextField portField = new JTextField(5);
	final JTextField databaseField = new JTextField(STRING_FIELDS_WIDTH);
	final JTextField userField = new JTextField(STRING_FIELDS_WIDTH);
	final JTextField passwordField = new JPasswordField(STRING_FIELDS_WIDTH);
	final JComboBox sslModeDropDown = new JComboBox( new String[] { "ssl=off", "ssl=basic", "ssl=peerAuthentication" } );
	
	final JTextField assertUserField = new JTextField(STRING_FIELDS_WIDTH);
	final JTextField domainField = new JTextField(STRING_FIELDS_WIDTH);

	final JLabel info = new JLabel(" ");
	final JButton submit = new JButton("Connect");
	final JButton submitAssert = new JButton("Connect (assert)");
	final JButton submitToken = new JButton("Connect (token)");

	private Thread connectionThread = null;
	
	private static final boolean TOKEN_SEC=false;

	public ConnectionTab(Dashboard container) {
		super(container);

		createConnectionFields();
		insertDefaultConnectionInfo();
	}
	
	private Field[] tInputFields = null;

	private void createConnectionFields() {
		pane.setLayout(new BoxLayout(pane, BoxLayout.PAGE_AXIS));

		String hostRegex = "[^\\s/]*"; // No spaces or "/" characters.
		String portRegex = "[0-9]{0,5}"; // 1-5 digits.
		String databaseRegex = ".+"; // Must not be empty.
		String userRegex = "[a-zA-Z0-9]+"; // Must not be empty.
		String passwordRegex = ".*"; // Can be anything.
		String domainRegex = "[A-Z\\.]*"; // Alphabetic

		Field.Warning emptyFieldWarning = new Field.Warning(".+", "This field must not be empty.");
		
		/* TODO: Clean me using security config parameter */		
		if (TOKEN_SEC) {
			tInputFields= new Field[]{
				new Field("Host", hostField, WARNING_ICON, hostRegex,
						emptyFieldWarning,
						new Field.Warning(hostRegex, "This is an invalid hostname.")),
				new Field("Port", portField, WARNING_ICON, portRegex,
								emptyFieldWarning,
								new Field.Warning(new Field.Validator() {
									public boolean validate(String text) {
										try {
											int port = Integer.parseInt(text);
											return (port >= 1 && port <= 65535);
										}
										catch (NumberFormatException e) {
											return false;
										}
									}
								}, "Valid ports range from 1 to 65535.")),
				new Field("Database", databaseField, WARNING_ICON, databaseRegex, emptyFieldWarning),
				new Field("User", userField, WARNING_ICON, userRegex, emptyFieldWarning),
				new Field("Password", passwordField, WARNING_ICON, passwordRegex),
				new Field("Asserted User", assertUserField, WARNING_ICON, userRegex, emptyFieldWarning),
				new Field("Domain", domainField, WARNING_ICON, domainRegex)};
			    //new Field("JAAS Authentication", jlJaasField, WARNING_ICON, "" )
		} else {
			tInputFields= new Field[]{
					new Field("Host", hostField, WARNING_ICON, hostRegex,
							emptyFieldWarning,
							new Field.Warning(hostRegex, "This is an invalid hostname.")),
					new Field("Port", portField, WARNING_ICON, portRegex,
									emptyFieldWarning,
									new Field.Warning(new Field.Validator() {
										public boolean validate(String text) {
											try {
												int port = Integer.parseInt(text);
												return (port >= 1 && port <= 65535);
											}
											catch (NumberFormatException e) {
												return false;
											}
										}
									}, "Valid ports range from 1 to 65535.")),
					new Field("Database", databaseField, WARNING_ICON, databaseRegex, emptyFieldWarning),
					new Field("User", userField, WARNING_ICON, userRegex, emptyFieldWarning),
					new Field("Password", passwordField, WARNING_ICON, passwordRegex)};
		};
		
		// submit (simple)
		submit.setActionCommand("submit");
		submit.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				connect(tInputFields);
			}
		});
		
		if (TOKEN_SEC) {
		// submit (assert)
		submitAssert.setActionCommand("submit_assert");
		submitAssert.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				connectAssert(tInputFields);
			}
		});
		// submit (token)
		submitToken.setActionCommand("submit_token");
		submitToken.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent event) {
				connectToken(tInputFields);
			}
		});
		}
		
		Insets padding = new Insets(Dashboard.BORDER_SIZE, 0, 0, Dashboard.BORDER_SIZE);

		GridBagConstraints labelConstraints = new GridBagConstraints();
		labelConstraints.anchor = GridBagConstraints.LINE_END;
		labelConstraints.gridx = 1;
		labelConstraints.insets = padding;

		GridBagConstraints fieldConstraints = new GridBagConstraints();
		fieldConstraints.anchor = GridBagConstraints.LINE_START;
		fieldConstraints.gridx = 2;
		fieldConstraints.gridwidth = 2;
		fieldConstraints.insets = padding;

		GridBagConstraints warningIconConstraints = new GridBagConstraints();
		warningIconConstraints.gridx = 4;
		warningIconConstraints.insets = padding;

		GridBagConstraints submitConstraints = new GridBagConstraints();
		submitConstraints.anchor = GridBagConstraints.LINE_END;
		submitConstraints.gridx = 3;
		submitConstraints.insets = padding;

		for (Field inputField : tInputFields) {
			connectionPanel.add(inputField.label, labelConstraints);
			if ( "Port:".equals(inputField.label.getText()) ) {
				fieldConstraints.gridwidth = 1;
				connectionPanel.add(inputField.inputField, fieldConstraints);
				fieldConstraints.gridwidth = 2;
				connectionPanel.add( sslModeDropDown, submitConstraints );
			} else
				connectionPanel.add(inputField.inputField, fieldConstraints);
			connectionPanel.add(inputField.warningIconLabel, warningIconConstraints);
		}

		connectionPanel.add(submit, submitConstraints);
		
		if (TOKEN_SEC) {
			connectionPanel.add(submitAssert, submitConstraints);
			connectionPanel.add(submitToken, submitConstraints);
		}
		
		pane.add(connectionPanel);

		JPanel infoPane = new JPanel();
		infoPane.setMinimumSize(new Dimension(0, 32));
		infoPane.add(info);
		pane.add(infoPane);

		add(pane, BorderLayout.CENTER);
	}

	private void insertDefaultConnectionInfo() {
		hostField.setText(GaianNode.DEFAULT_HOST);
		portField.setText(Integer.toString(GaianNode.DEFAULT_PORT));
		databaseField.setText(GaianDBConfig.GAIANDB_NAME);
		userField.setText(GaianDBConfig.GAIAN_NODE_DEFAULT_USR);
		passwordField.setText(GaianDBConfig.GAIAN_NODE_DEFAULT_PWD);
		assertUserField.setText(GaianDBConfig.GAIAN_NODE_DEFAULT_AUSER);
		domainField.setText(GaianDBConfig.GAIAN_NODE_DEFAULT_DOMAIN);
	}
	
	private static final byte CONNECT_MODE_STANDARD = 0;
	private static final byte CONNECT_MODE_ASSERT = 1;
	private static final byte CONNECT_MODE_TOKEN = 2;
	
	protected void connect(Field[] inputFields) {
		connect(inputFields, CONNECT_MODE_STANDARD);
	}
	
	protected void connectAssert(Field[] inputFields) {
		connect(inputFields, CONNECT_MODE_ASSERT);
	}
	
	protected void connectToken(Field[] inputFields) {
		/*
		 * This token connect involves a three-stage protocol.
		 * 1: Connect to Derby anonymously;
		 * 2: Send token to Derby via stored procedure/function, returning session ID;
		 * 3: Disconnect and reconnect with session ID.
		 */
		connect(inputFields, CONNECT_MODE_TOKEN);
	}
	
	private void connect(Field[] inputFields, byte connectMode) {
		
		if ( false == validate(inputFields) ) return;
		
		final String sslModeAssignment = (String) sslModeDropDown.getSelectedItem();
		final String url = 
			//				GaianDBConfig.getNetworkDriver().equals( GaianDBConfig.GDBUDP ) ?
			//					"jdbc:udp:derby://" + hostField.getText() + ":" + portField.getText() + "/" + databaseField.getText() :
			"jdbc:derby://" + hostField.getText() + ":" + portField.getText() + "/" + databaseField.getText() + ';' + sslModeAssignment;

		final Properties info = new Properties();
		
		switch (connectMode) {
			case CONNECT_MODE_STANDARD:
			default:
				info.setProperty(DBConnector.USERKEY, userField.getText());
				info.setProperty(DBConnector.PWDKEY, passwordField.getText());
				info.put(DBConnector.MODEKEY, DBConnector.MODE_SIMPLE);
				break;

			case CONNECT_MODE_ASSERT:
				info.setProperty(DBConnector.USERKEY, assertUserField.getText());
				info.setProperty(DBConnector.PROXYUIDKEY, userField.getText());
				info.setProperty(DBConnector.PROXYPWDKEY,passwordField.getText());
				info.put(DBConnector.MODEKEY, DBConnector.MODE_ASSERT);
				break;

			case CONNECT_MODE_TOKEN:
				info.setProperty(DBConnector.USERKEY, assertUserField.getText());  // "real user" ID
				info.setProperty(DBConnector.DOMAINKEY, domainField.getText());  // "real user" domain
				info.put(DBConnector.MODEKEY, DBConnector.MODE_TOKEN);
				break;
		}
		
		connect(url, info);
	}
	
	protected void connect( final String url, final Properties info ) {
		
		showMessage("Connecting...", LOADING_ICON);
		if (null != connectionThread) {
			connectionThread.interrupt();
		}

		connectionThread = new Thread(new Runnable() {
			public void run() {
				try {
					if (container.connect(url, info))
						showMessage("Connected to " + url + " successfully.");
					else
						showError("Could not connect to " + url + ".");
				}
				catch (ClassNotFoundException e) {
					showError("Could not load the Derby JDBC drivers.");
				}
				catch (Exception e1) {
					showError("Failed to connect to " + url + ": " + e1);
					e1.printStackTrace();
				}
				connectionThread = null;
			}
		},"DashboardConnection");
		connectionThread.start();
	}
	
	/**
	 * @param inputFields
	 * @return
	 */
	private boolean validate(Field[] inputFields) {
		hideMessage();

		boolean valid = true;
		for (Field inputField : inputFields) {
			if (valid) {
				valid = inputField.checkForWarnings();
				if (!valid) {
					inputField.inputField.requestFocusInWindow();
				}
			}
			else {
				inputField.checkForWarnings();
			}
		}
		return valid;
	}

	public void showMessage(String message) {
		showMessage(message, null);
	}

	public void showMessage(String message, Icon icon) {
		info.setText(message);
		info.setIcon(icon);
	}

	public void showError(String message) {
		showMessage(message, ERROR_ICON);
	}

	public void hideMessage() {
		info.setText(" ");
		info.setIcon(null);
	}

	public void connected(Connection newConn) {
	}

	public void disconnected() {
		showMessage("Disconnected.");
	}

	public void activated() {	    
	}

	public void deactivated() {	    
	}
}
