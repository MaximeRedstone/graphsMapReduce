/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strrchr.c                                       :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 15:35:01 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:16:57 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include <string.h>

char	*ft_strrchr(char *s, int c)
{
	char	*str;
	int		len;

	str = s;
	len = 0;
	while (str[len])
		len++;
	while (len > -1)
	{
		if (str[len] == c)
			return (str + len);
		len--;
	}
	return (str[len] == c ? str + len : NULL);
}
