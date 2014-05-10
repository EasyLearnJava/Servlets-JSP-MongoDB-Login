package com.easylearnjava.service;

import com.easylearnjava.dao.LoginDao;
import com.easylearnjava.exception.DaoException;
import com.easylearnjava.exception.ServiceException;
import com.easylearnjava.util.Constants;

public class LoginService {

	/**
	 * Method for implementing the business logic, like comparing the passwords
	 */
	public boolean isValidPassword(String userName, String password) {

		LoginDao loginDao = new LoginDao();

		try {
			String pwdFromDB = loginDao.getUserPassword(userName);
			if (null != pwdFromDB) {
				if (pwdFromDB.equals(password)) {
					return true;
				}
			}
			return false;
		} catch (Exception e) {
			if (!(e instanceof DaoException)) {
				e.printStackTrace();
			}
			throw new ServiceException(Constants.GLOBAL_EXCEPTION_MESSAGE);
		}
	}

}
