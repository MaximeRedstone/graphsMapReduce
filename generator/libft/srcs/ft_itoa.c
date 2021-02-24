/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_itoa.c                                          :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/03 13:18:08 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/10 17:40:11 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"
#include <stdlib.h>

char	*ft_itoa(long long n)
{
	int		pos;
	char	*res;

	pos = 0;
	while (n / ft_pow(10, pos) != 0 && pos < 10)
		pos++;
	pos = pos + (n < 1 ? 1 : 0);
	if (!(res = (char *)easymalloc(sizeof(*res) * (pos + 1))))
		return (NULL);
	res[pos] = '\0';
	res[0] = n < 0 ? '-' : 0;
	if (!n)
		res[0] = '0';
	else
	{
		while (pos > 0 && n != 0)
		{
			res[pos - 1] = 48 + (n < 0 ? -(n % 10) : n % 10);
			n /= 10;
			pos--;
		}
	}
	return (res);
}
