/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_ulltoa.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/03 13:18:08 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/10 17:40:11 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

#include "libft.h"

char	*ft_ulltoa(unsigned long long n)
{
	int						pos;
	char					*res;
	unsigned long long		len;

	pos = 1;
	len = n;
	while ((len /= 10))
		pos++;
	if (!(res = (char *)easymalloc(sizeof(*res) * (pos + 1))))
		return (NULL);
	res[pos] = 0;
	if (!n)
		res[0] = '0';
	while (pos > 0 && n)
	{
		res[--pos] = 48 + n % 10;
		n /= 10;
	}
	return (res);
}
