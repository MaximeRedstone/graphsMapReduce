/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strrev.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/02/07 13:34:06 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/06 21:10:00 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

char	*ft_strrev(char *o_str)
{
	char	tmp;
	int		pos;
	int		len;
	char	*str;

	if (!(str = o_str))
		return (NULL);
	pos = 0;
	len = ft_strlen(str);
	while (pos < (len >> 1))
	{
		tmp = str[len - 1 - pos];
		str[len - 1 - pos] = str[pos];
		str[pos] = tmp;
		pos++;
	}
	return (str);
}
