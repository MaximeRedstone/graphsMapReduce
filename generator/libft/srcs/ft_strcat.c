/* ************************************************************************** */
/*                                                                            */
/*                                                        :::      ::::::::   */
/*   ft_strcat.c                                        :+:      :+:    :+:   */
/*                                                    +:+ +:+         +:+     */
/*   By: sikpenou <marvin@42.fr>                    +#+  +:+       +#+        */
/*                                                +#+#+#+#+#+   +#+           */
/*   Created: 2019/04/05 15:03:44 by sikpenou          #+#    #+#             */
/*   Updated: 2019/04/11 19:28:31 by sikpenou         ###   ########.fr       */
/*                                                                            */
/* ************************************************************************** */

char	*ft_strcat(char *s1, const char *s2)
{
	int		pos;
	char	*dest;
	char	*src;

	dest = s1;
	src = (char *)s2;
	pos = 0;
	while (dest[pos] != '\0')
		pos++;
	while (src[0] != '\0')
	{
		dest[pos] = src[0];
		pos++;
		src++;
	}
	dest[pos] = '\0';
	return (dest);
}
