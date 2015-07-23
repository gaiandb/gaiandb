/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.awt.Container;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Properties;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;

public class ConfigurationDialog extends JDialog implements ActionListener {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	// Whether the user cancelled this dialog or not.
	private boolean cancelled=false, ok=false; //, closed=false;

	private static final long	serialVersionUID	= 1L;
	
	private final ActionListener owner;

	private JPanel				jContentPane		= null;
	
	private final String[] textFieldLabels;
	
	// All the text fields, accessed by text field label
	private final Hashtable<String,JTextField> configTextFields = new Hashtable<String,JTextField>();
	
	private final String configFileName;
	private File configFile;
	private final Properties configProperties = new Properties();

	private JButton okJButton = null;
	private JButton cancelJButton = null;
	private JLabel helpInfoJLabel = null;
	private JCheckBox rememberJCheckBox = null;
	
	/**
	 * @param owner
	 */
	public ConfigurationDialog(Container owner, String[] textFieldLabels) {
		this( owner, textFieldLabels, owner.getClass().getSimpleName() );
	}
	
	public ConfigurationDialog(Container owner, String[] textFieldLabels, String configID) {
		
		this.owner = (ActionListener) owner;
		this.textFieldLabels = textFieldLabels;
		for ( String label : textFieldLabels )
			configTextFields.put(label, (-1==label.toLowerCase().indexOf("password")?new JTextField():new JPasswordField()));

		this.setTitle("Configuration for " + configID);
		this.setSize(300, 230 + 45*textFieldLabels.length);
		this.setContentPane(getJContentPane());
		this.setLocationByPlatform(true);
		
		configFileName = configID.replaceAll(":", "#") + ".properties";  //replaceAll("\\/:\\*?<>|", "") );
		
		try {
			configFile = new File( configFileName );
			if ( configFile.exists() ) {
				FileInputStream fis = new FileInputStream( configFile );
				configProperties.clear();
				configProperties.load( fis );
				fis.close();
				
				if ( !configProperties.isEmpty() )
					rememberJCheckBox.setSelected(true);
				
				for ( String label : textFieldLabels )
					configTextFields.get(label).setText( configProperties.getProperty(label) );
			}
			
		} catch ( Exception e ) {
			String msg = e.toString();
			int exNameStartIndex = msg.lastIndexOf('.', msg.indexOf(':')) + 1;
			helpInfoJLabel.setText(msg.substring(exNameStartIndex));
		}
	}

	/**
	 * This method initializes jContentPane
	 * 
	 * @return javax.swing.JPanel
	 */
	private JPanel getJContentPane()
	{
		if (jContentPane == null)
		{
			jContentPane = new JPanel();
			jContentPane.setLayout(null);
//			jContentPane.add(getUsernamejTextField(), null);
			int yPos = 34;
			for ( String label : textFieldLabels ) {
		        JLabel jLabel = new JLabel();
		        jLabel.setBounds(new Rectangle(76, yPos, 150, 31));
		        jLabel.setText( label );
		        jContentPane.add(jLabel, null);
		        
		        JTextField jTextField = configTextFields.get(label);
		        jTextField.setBounds(new Rectangle(76, yPos+30, 146, 24));
		        jContentPane.add(jTextField);
//		        jTextField.addActionListener(this); - all parms are now validated via the ok button
		        
		        yPos += 60;
			}
			
			jContentPane.add(getRememberJCheckBox(yPos));
			yPos+=40;
			jContentPane.add(getOKJButton( yPos ), null);
			jContentPane.add(getCancelJButton( yPos ), null);
			yPos+=40;
			helpInfoJLabel = new JLabel();
			helpInfoJLabel.setBounds(new Rectangle(10, yPos, 1000, 31));
			jContentPane.add(helpInfoJLabel, null);
		}
		return jContentPane;
	}
	
	/**
	 * This method initializes rememberJCheckBox	
	 * 	
	 * @return javax.swing.JButton	
	 */
	private JCheckBox getRememberJCheckBox( int yPos )
	{
		if (rememberJCheckBox == null)
		{
			rememberJCheckBox = new JCheckBox( "remember" );
			rememberJCheckBox.setBounds(new Rectangle(100, yPos, 90, 27));
			rememberJCheckBox.addActionListener(this);
		}
		return rememberJCheckBox;
	}

	/**
	 * This method initializes okJButton	
	 * 	
	 * @return javax.swing.JButton	
	 */
	private JButton getOKJButton( int yPos )
	{
		if (okJButton == null)
		{
			okJButton = new JButton("OK");
			okJButton.setBounds(new Rectangle(40, yPos, 90, 27));
			okJButton.addActionListener(this);
		}
		return okJButton;
	}
	
	/**
	 * This method initializes canceljButton	
	 * 	
	 * @return javax.swing.JButton	
	 */
	private JButton getCancelJButton( int yPos )
	{
		if (cancelJButton == null)
		{
			cancelJButton = new JButton("CANCEL");
			cancelJButton.setBounds(new Rectangle(160, yPos, 90, 27));
			cancelJButton.addActionListener(this);
		}
		return cancelJButton;
	}

	public void actionPerformed(ActionEvent e)
	{
		Object src = e.getSource();
//		String actionCmd =e.getActionCommand();
		
		if(src==cancelJButton)
		{
			// then they have cancelled the logon
			this.cancelled=true;
			this.setVisible(false);
		}
		else if(src==okJButton)
		{		
			configProperties.clear();
			
			// Always set the properties
			for ( String label : textFieldLabels )
				configProperties.setProperty(label, configTextFields.get(label).getText());
			
//			System.out.println("prop file exists: " + configFile.exists() + ", cbox selected: " + rememberJCheckBox.isSelected());
			
			// store the properties - use an empty properties object to clear the file.
			if ( configFile.exists() || rememberJCheckBox.isSelected() ) {
				
				try {
					FileOutputStream fos = new FileOutputStream(configFile);
					if ( rememberJCheckBox.isSelected() )
						configProperties.store( fos, null );
					else {
						new Properties().store( fos, null );
					}
					fos.close();
					
				} catch ( IOException e1 ) {
					System.out.println("Unable to persist properties: " + e1);
				}
			}
			
			// Set the ok status and cascade the action to validate parms
			this.ok=true;
			owner.actionPerformed(e);
		}
	}
	
	public void requestNewConfigValues( String reason ) {
		System.out.println("msg = " + reason);
		helpInfoJLabel.setText( reason );		
		this.ok = false;
	}
	
	public Properties getConfigValuesIfReady()
	{
		if ( false == ok ) return null;
		return configProperties;
	}

	public boolean isCancelled()
	{
		return cancelled;
	}
		
//	public void close() {
//		closed = true;
//	}
//
//	public boolean isClosed() {
//		return closed;
//	}
	
}  //  @jve:decl-index=0:visual-constraint="144,46"
