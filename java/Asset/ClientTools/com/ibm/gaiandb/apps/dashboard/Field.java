/*
 * (C) Copyright IBM Corp. 2009
 *
 * LICENSE: Eclipse Public License v1.0
 * http://www.eclipse.org/legal/epl-v10.html
 */

package com.ibm.gaiandb.apps.dashboard;

import java.util.ArrayList;
import java.util.List;

import java.awt.Component;
import java.awt.Graphics;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;

import javax.swing.Icon;
import javax.swing.JLabel;
import javax.swing.JTextField;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.Document;
import javax.swing.text.PlainDocument;

public class Field {

//	Use PROPRIETARY notice if class contains a main() method, otherwise use COPYRIGHT notice.
	public static final String COPYRIGHT_NOTICE = "(c) Copyright IBM Corp. 2009";
	
	public JLabel label;
	public JTextField inputField;
	public JLabel warningIconLabel;

	public List<Warning> warnings = new ArrayList<Warning>();

	public Field(String title, JTextField field, Icon warningIcon, String inputRegex, Warning... warnings) {
		label = new JLabel(title + ":");

		inputField = field;
		inputField.setDocument(new ValidationDocument(new RegexValidator(inputRegex)));
		inputField.addFocusListener(new FocusListener() {
			public void focusGained(FocusEvent e) {}

			public void focusLost(FocusEvent e) {
				checkForWarnings();
			}
		});

		warningIconLabel = new JLabel(warningIcon);
		warningIconLabel.setDisabledIcon(new Icon() {
			public void paintIcon(Component c, Graphics g, int x, int y) {}

			public int getIconWidth() {
				return warningIconLabel.getIcon().getIconWidth();
			}

			public int getIconHeight() {
				return warningIconLabel.getIcon().getIconHeight();
			}
		});
		warningIconLabel.setEnabled(false);

		for (Warning warning : warnings) {
			addWarning(warning);
		}
	}

	public void addWarning(Warning warning) {
		warnings.add(warning);
	}

	public boolean checkForWarnings() {
		String text = inputField.getText();
		for (Warning warning : warnings) {
			if (!warning.validator.validate(text)) {
				warningIconLabel.setEnabled(true);
				warningIconLabel.setToolTipText(warning.message);
				return false;
			}
		}

		warningIconLabel.setEnabled(false);
		warningIconLabel.setToolTipText("");

		return true;
	}
	
	public static Document getValidatedDocument( String regex ) {
		return new ValidationDocument( new RegexValidator(regex) );
	}

	public static interface Validator {
		public boolean validate(String text);
	}

	public static class RegexValidator implements Validator {
		private String regex;

		public RegexValidator(String regex) {
			this.regex = regex;
		}

		public boolean validate(String text) {
			return text.matches(regex);
		}
	}

	private static class ValidationDocument extends PlainDocument {
		private static final long serialVersionUID = -4871624484682802518L;
		private final Validator validator;

		public ValidationDocument(Validator validator) {
			super();
			this.validator = validator;
		}

		public void insertString(int offs, String str, AttributeSet a) throws BadLocationException {
			if (null == str) {
				return;
			}

			String newStr = getText(0, offs) + str + getText(offs, getLength() - offs);
			if (validator.validate(newStr)) {
				super.insertString(offs, str, a);
			}
		}
	}

	public static class Warning {
		public Validator validator;
		public String message;

		public Warning(Validator validator, String message) {
			init(validator, message);
		}

		public Warning(String regex, String message) {
			init(new RegexValidator(regex), message);
		}

		private void init(Validator validator, String message) {
			this.validator = validator;
			this.message = message;
		}
	}
}
