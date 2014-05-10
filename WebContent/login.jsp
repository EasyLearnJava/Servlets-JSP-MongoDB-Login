<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
	pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Servlet JSP Login</title>

<script type="text/javascript">
	function validateData() {
		var userNameValue = document.getElementById('userid').value.trim();
		if (userNameValue == "") {
			alert('please enter username');
			document.getElementById('userid').focus();
			return false;
		}
		var passwordField = document.getElementById('password').value.trim();
		if (passwordField == "") {
			alert('please enter password');
			document.getElementById('password').focus();
			return false;
		}
	}
</script>

</head>
<body>
	<h2 align="center">Login functionality implemented using Servlet,
		JSP and MongoDB</h2>
	<form action="login" method="post">
		<table align="center">
			<tr>
				<td class="right">username :</td>
				<td class="left"><input type="text" name="usernameTB" id="userid"></td>
			</tr>
			<tr>
				<td class="right">password :</td>
				<td class="left"><input type="password" name="passwordTB" id="password"><br></td>
			</tr>
			<tr>
				<td class="right"><input type="submit" value="login" onclick="return validateData();"></td>
				<td style="color: red">
					<%
						String errorMessage = (String) request.getAttribute("errMsg");
						if (errorMessage != null) {
					%> <%=errorMessage%> <%
 	}
 %>
				</td>
			</tr>
		</table>
	</form>
</body>
</html>